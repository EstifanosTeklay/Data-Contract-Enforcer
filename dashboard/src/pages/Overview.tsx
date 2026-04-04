import { Pie, PieChart, Cell, ResponsiveContainer } from "recharts";
import { HealthScoreCard } from "../components/HealthScoreCard";
import { useHealthData } from "../hooks/useHealthData";

const severityPalette: Record<string, string> = {
  CRITICAL: "#ef4444",
  HIGH: "#f97316",
  MEDIUM: "#eab308",
  LOW: "#22c55e"
};

function metricCard(label: string, value: number) {
  return (
    <div className="panel p-4">
      <p className="text-xs uppercase tracking-wide text-slate-400">{label}</p>
      <p className="mt-2 text-2xl font-bold text-slate-100">{value}</p>
    </div>
  );
}

export function Overview() {
  const { data, isLoading } = useHealthData();

  if (isLoading || !data) {
    return (
      <div className="space-y-4">
        <div className="skeleton h-40" />
        <div className="grid gap-4 md:grid-cols-4">
          <div className="skeleton h-24" />
          <div className="skeleton h-24" />
          <div className="skeleton h-24" />
          <div className="skeleton h-24" />
        </div>
      </div>
    );
  }

  const donutData = Object.entries(data.violations_by_severity).map(([name, value]) => ({ name, value }));

  return (
    <div className="space-y-4">
      <div className="panel-gradient p-5">
        <p className="text-xs uppercase tracking-[0.2em] text-indigo-200/70">System Signal</p>
        <p className="mt-2 text-sm text-slate-200">
          {data.summary.failed > 0
            ? "Active data quality violations detected. Teams should prioritize red severity items first."
            : "No active violations detected. Data contracts are currently stable."}
        </p>
      </div>

      <HealthScoreCard score={data.health_score} />

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4 animate-fadeInUp">
        {metricCard("Total Checks", data.summary.total_checks)}
        {metricCard("Passed", data.summary.passed)}
        {metricCard("Failed", data.summary.failed)}
        {metricCard("Contracts Monitored", data.summary.contracts_monitored)}
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_auto]">
        <div className="panel p-4 md:p-5">
          <p className="text-sm text-slate-300">Violations by Severity</p>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={donutData} dataKey="value" nameKey="name" innerRadius={68} outerRadius={96}>
                  {donutData.map((entry) => (
                    <Cell key={entry.name} fill={severityPalette[entry.name] || "#64748b"} />
                  ))}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-300">
            {Object.entries(severityPalette).map(([name, color]) => (
              <div key={name} className="flex items-center gap-2">
                <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color }} />
                <span>{name}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="panel p-4 md:w-72">
          <p className="text-xs uppercase tracking-wide text-slate-400">Last Updated</p>
          <p className="mt-3 text-sm text-slate-200">{new Date(data.last_updated).toLocaleString()}</p>
          <p className="mt-2 text-xs text-slate-500">Auto-refreshes every 30 seconds.</p>
        </div>
      </div>
    </div>
  );
}
