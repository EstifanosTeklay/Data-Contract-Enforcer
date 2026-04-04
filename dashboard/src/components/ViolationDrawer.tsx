import { Violation } from "../types";
import { BlastRadiusGraph } from "./BlastRadiusGraph";

interface ViolationDrawerProps {
  violation: Violation | null;
  onClose: () => void;
}

export function ViolationDrawer({ violation, onClose }: ViolationDrawerProps) {
  if (!violation) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/50">
      <div className="h-full w-full max-w-2xl overflow-y-auto bg-card p-5 md:p-7">
        <div className="mb-6 flex items-start justify-between">
          <div>
            <h3 className="text-lg font-semibold">Violation Details</h3>
            <p className="text-sm text-slate-400">{violation.violation_id}</p>
          </div>
          <button onClick={onClose} className="rounded-md bg-slate-800 px-3 py-1 text-sm text-slate-200">
            Close
          </button>
        </div>

        <div className="space-y-6">
          <section className="card p-4">
            <p className="mb-2 text-sm font-medium text-slate-300">Message</p>
            <p className="text-sm text-slate-200">{violation.message}</p>
          </section>

          <section className="card p-4">
            <p className="mb-3 text-sm font-medium text-slate-300">Blame Chain</p>
            <ol className="space-y-3">
              {violation.blame_chain.length === 0 && (
                <li className="text-sm text-slate-400">No blame chain available.</li>
              )}
              {violation.blame_chain.map((node, idx) => (
                <li key={`${node.commit_hash}-${idx}`} className="rounded-lg border border-slate-700/60 p-3 text-sm">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="font-semibold text-indigo-300">#{node.rank || idx + 1} - {node.commit_hash}</p>
                    <span className="text-xs text-slate-400">Confidence {Math.round((node.confidence || 0) * 100)}%</span>
                  </div>
                  <p className="mt-1 text-slate-300">{node.author} - {node.file_path}</p>
                </li>
              ))}
            </ol>
          </section>

          <section className="card p-4">
            <p className="mb-3 text-sm font-medium text-slate-300">Blast Radius</p>
            <div className="mb-3 flex flex-wrap gap-2">
              {violation.blast_radius.affected_pipelines.map((pipeline) => (
                <span key={pipeline} className="rounded-full bg-slate-800 px-2.5 py-0.5 text-xs text-slate-300">
                  {pipeline}
                </span>
              ))}
            </div>
            <p className="mb-4 text-sm text-slate-400">Affected nodes: {violation.blast_radius.affected_nodes_count}</p>
            <BlastRadiusGraph violation={violation} />
          </section>
        </div>
      </div>
    </div>
  );
}
