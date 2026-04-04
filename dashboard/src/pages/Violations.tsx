import { useEffect, useMemo, useState } from "react";
import { LiveIndicator } from "../components/LiveIndicator";
import { ViolationDrawer } from "../components/ViolationDrawer";
import { ViolationsTable } from "../components/ViolationsTable";
import { useLiveViolations } from "../hooks/useLiveViolations";
import { useViolations } from "../hooks/useViolations";
import { Violation } from "../types";

function mergeViolations(base: Violation[], incoming: Violation): Violation[] {
  const map = new Map(base.map((v) => [v.violation_id, v]));
  map.set(incoming.violation_id, incoming);
  return [...map.values()].sort(
    (a, b) => new Date(b.detected_at || 0).getTime() - new Date(a.detected_at || 0).getTime()
  );
}

export function Violations() {
  const { data, isLoading } = useViolations();
  const { latest, isConnected } = useLiveViolations();
  const [rows, setRows] = useState<Violation[]>([]);
  const [selected, setSelected] = useState<Violation | null>(null);
  const [toast, setToast] = useState<string | null>(null);

  useEffect(() => {
    if (data) {
      setRows(data);
    }
  }, [data]);

  useEffect(() => {
    if (!latest) return;
    setRows((prev) => mergeViolations(prev, latest));
    const systemPart = latest.system && latest.system.toLowerCase() !== "unknown" ? ` in ${latest.system}` : "";
    setToast(`New ${latest.severity} violation detected${systemPart}`);
    const timer = window.setTimeout(() => setToast(null), 3500);
    return () => window.clearTimeout(timer);
  }, [latest]);

  const content = useMemo(() => {
    if (isLoading) {
      return <div className="skeleton h-72" />;
    }

    if (rows.length === 0) {
      return (
        <div className="card p-8 text-center text-sm text-slate-400">
          No violations yet. Live events will appear here as they are detected.
        </div>
      );
    }

    return <ViolationsTable rows={rows} onSelect={setSelected} />;
  }, [isLoading, rows]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Violations</h2>
        <LiveIndicator active={isConnected} />
      </div>

      {content}
      <ViolationDrawer violation={selected} onClose={() => setSelected(null)} />

      {toast && (
        <div className="fixed bottom-4 right-4 z-50 rounded-lg border border-indigo-400/40 bg-indigo-500/20 px-4 py-3 text-sm text-indigo-100 shadow-lg">
          {toast}
        </div>
      )}
    </div>
  );
}
