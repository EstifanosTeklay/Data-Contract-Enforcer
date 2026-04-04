import { useMemo, useState } from "react";
import { Severity, Violation } from "../types";

interface ViolationsTableProps {
  rows: Violation[];
  onSelect: (violation: Violation) => void;
}

const severityColors: Record<Severity, string> = {
  CRITICAL: "bg-red-500/20 text-red-300",
  HIGH: "bg-orange-500/20 text-orange-300",
  MEDIUM: "bg-yellow-500/20 text-yellow-300",
  LOW: "bg-blue-500/20 text-blue-300"
};

export function ViolationsTable({ rows, onSelect }: ViolationsTableProps) {
  const [severityFilter, setSeverityFilter] = useState<"ALL" | Severity>("ALL");
  const [sortDesc, setSortDesc] = useState(true);
  const [search, setSearch] = useState("");

  function exportCsv(records: Violation[]) {
    const header = [
      "violation_id",
      "severity",
      "system",
      "failing_field",
      "check_type",
      "records_failing",
      "detected_at",
      "injected"
    ];
    const lines = records.map((r) =>
      [
        r.violation_id,
        r.severity,
        r.system,
        r.failing_field,
        r.check_type,
        String(r.records_failing),
        r.detected_at || "",
        String(r.injected)
      ]
        .map((field) => `"${String(field).replace(/"/g, '""')}"`)
        .join(",")
    );
    const csv = [header.join(","), ...lines].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `violations-${new Date().toISOString().replace(/[.:]/g, "-")}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  const filtered = useMemo(() => {
    const base = severityFilter === "ALL" ? rows : rows.filter((r) => r.severity === severityFilter);
    const searched = search.trim()
      ? base.filter((r) =>
          [r.system, r.failing_field, r.check_type, r.violation_id]
            .join(" ")
            .toLowerCase()
            .includes(search.toLowerCase())
        )
      : base;

    return [...searched].sort((a, b) => {
      const aTime = new Date(a.detected_at || 0).getTime();
      const bTime = new Date(b.detected_at || 0).getTime();
      return sortDesc ? bTime - aTime : aTime - bTime;
    });
  }, [rows, severityFilter, sortDesc, search]);

  return (
    <div className="panel p-4 md:p-5 animate-fadeInUp">
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search by system, field, check, or ID"
          className="min-w-64 rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm"
        />
        <select
          value={severityFilter}
          onChange={(event) => setSeverityFilter(event.target.value as "ALL" | Severity)}
          className="rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm"
        >
          <option value="ALL">All Severities</option>
          <option value="CRITICAL">Critical</option>
          <option value="HIGH">High</option>
          <option value="MEDIUM">Medium</option>
          <option value="LOW">Low</option>
        </select>

        <button
          onClick={() => setSortDesc((s) => !s)}
          className="rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm"
        >
          Sort Detected At: {sortDesc ? "Newest" : "Oldest"}
        </button>

        <button
          onClick={() => exportCsv(filtered)}
          className="rounded-md border border-indigo-400/40 bg-indigo-500/10 px-3 py-2 text-sm text-indigo-200"
        >
          Export CSV
        </button>
      </div>

      {filtered.length === 0 ? (
        <p className="rounded-lg border border-dashed border-slate-700 p-8 text-center text-sm text-slate-400">
          No violations available for the current filter.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="py-2 pr-3">Severity</th>
                <th className="py-2 pr-3">Failing Field</th>
                <th className="py-2 pr-3">Check Type</th>
                <th className="py-2 pr-3">Records Failing</th>
                <th className="py-2 pr-3">Detected At</th>
                <th className="py-2">Injected</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row) => (
                <tr
                  key={row.violation_id}
                  onClick={() => onSelect(row)}
                  className="cursor-pointer border-t border-slate-800/80 hover:bg-slate-800/35"
                >
                  <td className="py-2.5 pr-3">
                    <span className={`badge ${severityColors[row.severity]}`}>{row.severity}</span>
                  </td>
                  <td className="py-2.5 pr-3 text-slate-300">{row.failing_field}</td>
                  <td className="py-2.5 pr-3 text-slate-300">{row.check_type}</td>
                  <td className="py-2.5 pr-3 text-slate-200">{row.records_failing}</td>
                  <td className="py-2.5 pr-3 text-slate-300">
                    {row.detected_at ? new Date(row.detected_at).toLocaleString() : "-"}
                  </td>
                  <td className="py-2.5">
                    {row.injected ? (
                      <span className="badge bg-fuchsia-500/20 text-fuchsia-300">Injected</span>
                    ) : (
                      "-"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
