import { useMemo, useState } from "react";
import { BarChart, Bar, Cell, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";
import { useEnforcementReport } from "../hooks/useEnforcementReport";

const severityColor: Record<string, string> = {
  CRITICAL: "#ef4444",
  HIGH: "#f97316",
  MEDIUM: "#facc15",
  LOW: "#22c55e"
};

function humanizeAction(text: string): string {
  return text
    .replace(/^Update\s+/i, "Please update ")
    .replace(/\bfix contract clause\b/i, "resolve contract rule")
    .replace(/currently outputs/gi, "currently produces")
    .replace(/Blamed commit:/gi, "Likely introduced in commit")
    .replace(/Downstream consumers affected:/gi, "Affected downstream systems:")
    .replace(/\s+/g, " ")
    .trim();
}

function humanizeViolationSummary(text: string): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  const systemMatch = normalized.match(/^The (.+?) produced data where the field '([^']+)' failed a ([^.]+?)\./i);
  const rangeMatch = normalized.match(/confidence is in ([^,]+) range, not ([^.]+)\./i);
  const downstreamMatch = normalized.match(/Downstream systems affected: (.+)\.?/i);

  if (systemMatch) {
    const [, system, field, checkType] = systemMatch;
    const parts = [
      `${system} is sending values that break the expected ${field} ${checkType}.`
    ];

    if (rangeMatch) {
      const [, actualRange, expectedRange] = rangeMatch;
      parts.push(`The data is arriving on a ${actualRange} scale, but downstream contracts expect ${expectedRange}.`);
    }

    if (/Breaking change detected\./i.test(normalized)) {
      parts.push("This is a breaking change and needs coordination before the next release.");
    }

    if (downstreamMatch) {
      parts.push(`Teams affected: ${downstreamMatch[1]}.`);
    }

    return parts.join(" ");
  }

  return normalized
    .replace(/^The (.+?) produced data where the field '([^']+)' failed a ([^.]+?)\./i, "$1 has a contract issue on $2 because it failed a $3.")
    .replace(/Breaking change detected\./gi, "This is a breaking change.")
    .replace(/Downstream systems affected:/gi, "Teams affected:")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanMarkdownInline(text: string): string {
  return text
    .replace(/\*\*(.*?)\*\*/g, "$1")
    .replace(/^\*(.*?)\*$/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[(.*?)\]\((.*?)\)/g, "$1")
    .trim();
}

type NarrativeSection = {
  title: string;
  eyebrow?: string;
  body?: string[];
  bullets?: string[];
  stats?: Array<{ label: string; value: string }>;
};

function sentenceCase(value: string): string {
  return value
    .toLowerCase()
    .replace(/(^\w|[\s_-]\w)/g, (match) => match.toUpperCase())
    .replace(/[_-]/g, " ");
}

function buildNarrativeSections(report: NonNullable<ReturnType<typeof useEnforcementReport>["data"]>["data"], markdown: string): NarrativeSection[] {
  const sections: NarrativeSection[] = [];
  const health = report.section_1_data_health;
  const violations = report.section_2_violations;
  const schemaChanges = report.section_3_schema_changes?.schema_changes || [];
  const aiRisk = report.section_4_ai_risk;
  const recommendations = report.section_5_recommendations || [];

  if (health) {
    sections.push({
      eyebrow: "Current picture",
      title: "What leaders need to know right now",
      body: [
        cleanMarkdownInline(
          health.narrative ||
            "Platform reliability needs attention, with contract violations affecting monitored systems."
        ),
        `Across ${health.contracts_monitored ?? 0} monitored contracts, ${health.total_passed ?? 0} checks passed and ${health.total_failed ?? 0} checks failed during this reporting window.`
      ],
      stats: [
        { label: "Health score", value: `${health.data_health_score ?? 0}/100` },
        { label: "Critical issues", value: String(health.critical_violations ?? 0) },
        { label: "Checks run", value: String(health.total_checks ?? 0) }
      ]
    });
  }

  if ((violations?.top_3_violations || []).length > 0) {
    sections.push({
      eyebrow: "Urgent issues",
      title: "Where the biggest delivery risk sits",
      bullets: (violations?.top_3_violations || []).slice(0, 3).map((item) => {
        const summary = humanizeViolationSummary(cleanMarkdownInline(item.plain_language));
        return `${item.system}: ${summary}`;
      })
    });
  }

  if (schemaChanges.length > 0) {
    sections.push({
      eyebrow: "Change management",
      title: "Schema changes that need coordination",
      bullets: schemaChanges.slice(0, 3).map((change) => {
        const prefix = `${change.system} is marked ${sentenceCase(change.compatibility_verdict)}.`;
        const followUp = change.action_required
          ? cleanMarkdownInline(change.action_required)
          : "Review downstream compatibility before release.";
        return `${prefix} ${followUp}`;
      })
    });
  }

  if (aiRisk) {
    sections.push({
      eyebrow: "AI controls",
      title: "Model and prompt risk posture",
      body: [
        `Overall AI risk is currently assessed as ${sentenceCase(aiRisk.overall_ai_risk || "unknown")}.`,
        cleanMarkdownInline(aiRisk.embedding_drift?.narrative || ""),
        cleanMarkdownInline(aiRisk.prompt_validation?.narrative || ""),
        cleanMarkdownInline(aiRisk.output_schema?.narrative || "")
      ].filter(Boolean)
    });
  }

  if (recommendations.length > 0) {
    sections.push({
      eyebrow: "Next steps",
      title: "Recommended actions for delivery teams",
      bullets: recommendations.slice(0, 3).map((rec) => {
        const action = humanizeAction(cleanMarkdownInline(rec.action));
        const impact = cleanMarkdownInline(rec.estimated_impact);
        return `${rec.system}: ${action} ${impact}`;
      })
    });
  }

  const footerNote = markdown
    .split("\n")
    .map((line) => line.trim())
    .find((line) => line.startsWith("*This report"));

  if (footerNote) {
    sections.push({
      eyebrow: "Source note",
      title: "How this brief was assembled",
      body: [cleanMarkdownInline(footerNote)]
    });
  }

  return sections;
}

export function EnforcementReport() {
  const { data, isLoading } = useEnforcementReport();
  const [copied, setCopied] = useState<string | null>(null);

  const report = data?.data;
  const severityRows = useMemo(() => {
    const map = report?.section_2_violations?.by_severity || {};
    return Object.entries(map).map(([name, value]) => ({ name, value }));
  }, [report]);
  const narrativeSections = useMemo(() => {
    if (!report) return [];
    return buildNarrativeSections(report, data?.markdown || "");
  }, [report, data?.markdown]);

  if (isLoading) {
    return (
      <div className="grid gap-4 lg:grid-cols-3">
        <div className="skeleton h-44" />
        <div className="skeleton h-44" />
        <div className="skeleton h-44" />
      </div>
    );
  }

  if (!data || !report || Object.keys(report).length === 0) {
    return (
      <div className="panel p-8 text-center text-sm text-slate-400">
        Enforcement report is not available yet. Run report generation and refresh.
      </div>
    );
  }

  const score = report.section_1_data_health?.data_health_score ?? 0;
  const scoreClass = score >= 80 ? "text-emerald-300" : score >= 60 ? "text-yellow-300" : "text-red-300";

  return (
    <div className="space-y-5">
      <div className="panel-gradient p-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-indigo-200/70">Enforcement Brief</p>
            <h3 className="mt-2 text-2xl font-bold text-white">Data Reliability Executive Brief</h3>
            <p className="mt-2 text-sm text-slate-300">
              Generated {data.generated_at ? new Date(data.generated_at).toLocaleString() : "unknown time"}
            </p>
          </div>
          <div className="rounded-2xl border border-slate-700/70 bg-slate-950/50 px-5 py-3 text-right">
            <p className="text-xs text-slate-400">Data Health Score</p>
            <p className={`text-4xl font-extrabold ${scoreClass}`}>{score}</p>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <div className="panel p-4">
          <p className="text-xs uppercase tracking-wide text-slate-400">Total Violations</p>
          <p className="mt-2 text-2xl font-bold text-slate-100">{report.section_2_violations?.total_violations ?? 0}</p>
        </div>
        <div className="panel p-4">
          <p className="text-xs uppercase tracking-wide text-slate-400">Critical Issues</p>
          <p className="mt-2 text-2xl font-bold text-red-300">{report.section_1_data_health?.critical_violations ?? 0}</p>
        </div>
        <div className="panel p-4">
          <p className="text-xs uppercase tracking-wide text-slate-400">Checks Passed</p>
          <p className="mt-2 text-2xl font-bold text-emerald-300">{report.section_1_data_health?.total_passed ?? 0}</p>
        </div>
        <div className="panel p-4">
          <p className="text-xs uppercase tracking-wide text-slate-400">Contracts Tracked</p>
          <p className="mt-2 text-2xl font-bold text-sky-300">{report.section_1_data_health?.contracts_monitored ?? 0}</p>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.15fr_1fr]">
        <section className="panel p-5">
          <h4 className="text-sm uppercase tracking-wide text-slate-400">Violations by Severity</h4>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={severityRows} margin={{ top: 20, right: 12, left: 0, bottom: 0 }}>
                <XAxis dataKey="name" stroke="#94a3b8" />
                <YAxis allowDecimals={false} stroke="#94a3b8" />
                <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151" }} />
                <Bar dataKey="value" radius={[6, 6, 0, 0]}>
                  {severityRows.map((item) => (
                    <Cell key={item.name} fill={severityColor[item.name] || "#6366f1"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="panel p-5">
          <h4 className="text-sm uppercase tracking-wide text-slate-400">Top Violations</h4>
          <div className="mt-3 space-y-3">
            {(report.section_2_violations?.top_3_violations || []).slice(0, 3).map((v) => (
              <article key={v.violation_id} className="rounded-xl border border-slate-700/70 bg-slate-900/70 p-4">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <span className={`badge ${v.severity === "CRITICAL" ? "bg-red-500/20 text-red-300" : "bg-amber-500/20 text-amber-300"}`}>
                    {v.severity}
                  </span>
                  <span className="text-xs text-slate-400">{v.system}</span>
                </div>
                <p className="text-sm leading-7 text-slate-200">{humanizeViolationSummary(v.plain_language)}</p>
              </article>
            ))}
            {(report.section_2_violations?.top_3_violations || []).length === 0 && (
              <p className="text-sm text-slate-400">No top violations listed in this report.</p>
            )}
          </div>
        </section>
      </div>

      <section className="panel p-5">
        <h4 className="text-sm uppercase tracking-wide text-slate-400">Priority Actions For Teams</h4>
        <div className="mt-3 space-y-3">
          {(report.section_5_recommendations || []).map((rec) => (
            <article key={`${rec.priority}-${rec.contract_clause}`} className="rounded-xl border border-slate-700/70 bg-slate-900/65 p-5 shadow-lg shadow-black/20">
              <div className="mb-2 flex flex-wrap items-center gap-2">
                <span className="badge bg-indigo-500/20 text-indigo-300">Priority {rec.priority}</span>
                <span className="badge bg-slate-700/60 text-slate-200">{rec.risk_level}</span>
                <span className="text-xs text-slate-400">{rec.system}</span>
              </div>
              <p className="text-sm text-slate-100 leading-7">{humanizeAction(rec.action)}</p>
              <p className="mt-2 text-xs text-slate-400">Impact: {rec.estimated_impact}</p>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  className="rounded-md border border-slate-600 px-3 py-1.5 text-xs text-slate-100 hover:bg-slate-800"
                  onClick={async () => {
                    await navigator.clipboard.writeText(rec.action);
                    setCopied(rec.contract_clause);
                    window.setTimeout(() => setCopied(null), 1300);
                  }}
                >
                  {copied === rec.contract_clause ? "Copied" : "Copy Action"}
                </button>
                <span className="rounded-md bg-slate-800 px-3 py-1.5 text-xs text-slate-300">{rec.file_path}</span>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="panel p-5">
        <h4 className="text-sm uppercase tracking-wide text-slate-400">Narrative Brief</h4>
        <div className="mt-3 max-h-[34rem] space-y-4 overflow-auto rounded-xl border border-slate-700/70 bg-slate-950/70 p-4 md:p-5">
          {narrativeSections.length === 0 && (
            <p className="text-sm text-slate-400">No narrative markdown report found.</p>
          )}
          {narrativeSections.map((section, idx) => {
            const isLead = idx === 0;
            return (
              <article
                key={`brief-${idx}`}
                className={`rounded-2xl border p-4 ${isLead ? "border-indigo-400/25 bg-indigo-500/8" : "border-slate-700/70 bg-slate-900/65"}`}
              >
                {section.eyebrow && (
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-indigo-300/85">{section.eyebrow}</p>
                )}
                <h5 className="mt-1 text-base font-semibold text-slate-100">{section.title}</h5>
                {section.stats && section.stats.length > 0 && (
                  <div className="mt-3 grid gap-2 sm:grid-cols-3">
                    {section.stats.map((stat) => (
                      <div key={stat.label} className="rounded-xl border border-slate-700/60 bg-slate-950/55 px-3 py-2">
                        <p className="text-[11px] uppercase tracking-wide text-slate-400">{stat.label}</p>
                        <p className="mt-1 text-sm font-semibold text-slate-100">{stat.value}</p>
                      </div>
                    ))}
                  </div>
                )}
                {section.body && section.body.length > 0 && (
                  <div className="mt-3 space-y-2">
                    {section.body.map((paragraph, paragraphIndex) => (
                      <p key={`${section.title}-body-${paragraphIndex}`} className="text-sm leading-7 text-slate-300">
                        {paragraph}
                      </p>
                    ))}
                  </div>
                )}
                {section.bullets && section.bullets.length > 0 && (
                  <div className="mt-3 space-y-2">
                    {section.bullets.map((bullet, bulletIndex) => (
                      <div
                        key={`${section.title}-bullet-${bulletIndex}`}
                        className="rounded-xl border border-slate-800/80 bg-slate-950/45 px-3 py-2.5 text-sm leading-7 text-slate-300"
                      >
                        <span className="mr-2 text-indigo-300">•</span>
                        {bullet}
                      </div>
                    ))}
                  </div>
                )}
              </article>
            );
          })}
        </div>
      </section>
    </div>
  );
}
