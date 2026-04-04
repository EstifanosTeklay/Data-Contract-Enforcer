import { useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";
import { ContractItem } from "../types";

interface ContractCardsProps {
  contracts: ContractItem[];
}

export function ContractCards({ contracts }: ContractCardsProps) {
  const [selected, setSelected] = useState<ContractItem | null>(null);
  const [showYaml, setShowYaml] = useState(false);

  if (contracts.length === 0) {
    return (
      <div className="card p-8 text-center text-sm text-slate-400">
        No generated contracts are available yet.
      </div>
    );
  }

  return (
    <>
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3 animate-fadeInUp">
        {contracts.map((contract) => (
          <article key={contract.contract_id} className="card p-4">
            <p className="text-sm text-slate-400">{contract.contract_id}</p>
            <p className="mt-1 text-sm text-slate-300">Owner: {contract.owner}</p>
            <p className="text-sm text-slate-300">Clauses: {contract.clause_count}</p>
            <p className="text-sm text-slate-300">
              Last validated: {contract.last_validated ? new Date(contract.last_validated).toLocaleString() : "-"}
            </p>

            <div className="mt-3">
              <div className="mb-1 flex justify-between text-xs text-slate-400">
                <span>Pass Rate</span>
                <span>{Math.round(contract.pass_rate * 100)}%</span>
              </div>
              <div className="h-2 rounded-full bg-slate-800">
                <div className="h-2 rounded-full bg-emerald-500" style={{ width: `${Math.round(contract.pass_rate * 100)}%` }} />
              </div>
            </div>

            <div className="mt-4 flex items-center gap-2">
              <button
                className="rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white"
                onClick={() => {
                  setSelected(contract);
                  setShowYaml(false);
                }}
              >
                View Contract
              </button>
              {contract.dbt_counterpart && (
                <a
                  href={`http://localhost:8000/api/contracts/${contract.contract_id}/dbt`}
                  download
                  className="rounded-md border border-slate-600 px-3 py-1.5 text-xs text-slate-200"
                >
                  Download dbt
                </a>
              )}
            </div>
          </article>
        ))}
      </div>

      {selected && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
          <div className="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-xl border border-slate-700 bg-card p-4">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-slate-100">{selected.contract_id}</h3>
              <div className="flex items-center gap-2">
                <button
                  className="rounded-md bg-slate-800 px-3 py-1 text-xs"
                  onClick={() => setShowYaml((s) => !s)}
                >
                  {showYaml ? "Show Summary" : "Show YAML"}
                </button>
                <button className="rounded-md bg-slate-800 px-3 py-1 text-xs" onClick={() => setSelected(null)}>
                  Close
                </button>
              </div>
            </div>
            {showYaml ? (
              <SyntaxHighlighter language="yaml" style={oneDark} customStyle={{ margin: 0, borderRadius: 8 }}>
                {selected.yaml}
              </SyntaxHighlighter>
            ) : (
              <div className="rounded-xl border border-slate-700/70 bg-slate-900/70 p-4">
                <p className="mb-2 text-xs uppercase tracking-wide text-slate-400">Human-readable Contract Summary</p>
                <pre className="whitespace-pre-wrap text-sm leading-6 text-slate-100">{selected.human_summary}</pre>
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
