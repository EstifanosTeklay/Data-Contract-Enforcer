import { RadialBar, RadialBarChart, ResponsiveContainer } from "recharts";
import { AIMetricsResponse } from "../types";

interface AIMetricsCardsProps {
  metrics: AIMetricsResponse;
}

const riskColors: Record<string, string> = {
  HIGH: "bg-red-500/20 text-red-300",
  MEDIUM: "bg-yellow-500/20 text-yellow-300",
  LOW: "bg-emerald-500/20 text-emerald-300"
};

export function AIMetricsCards({ metrics }: AIMetricsCardsProps) {
  const gaugeValue = Math.min(100, (metrics.embedding_drift.score / 0.3) * 100);
  const thresholdPercent = (metrics.embedding_drift.threshold / 0.3) * 100;

  return (
    <div className="animate-fadeInUp space-y-4">
      <div>
        <span className={`badge ${riskColors[metrics.overall_risk] || "bg-slate-700 text-slate-200"}`}>
          Overall AI Risk: {metrics.overall_risk}
        </span>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <div className="card p-4">
          <p className="text-sm text-slate-400">Embedding Drift</p>
          <div className="h-40">
            <ResponsiveContainer width="100%" height="100%">
              <RadialBarChart data={[{ value: gaugeValue }]} startAngle={180} endAngle={0} innerRadius="70%" outerRadius="100%">
                <RadialBar dataKey="value" cornerRadius={12} fill="#6366f1" />
              </RadialBarChart>
            </ResponsiveContainer>
          </div>
          <div className="relative mt-1 h-2 rounded-full bg-slate-800">
            <div className="h-2 rounded-full bg-indigo-500" style={{ width: `${gaugeValue}%` }} />
            <div className="absolute top-0 h-2 w-0.5 bg-red-400" style={{ left: `${thresholdPercent}%` }} />
          </div>
          <p className="mt-3 text-sm text-slate-200">Score {metrics.embedding_drift.score.toFixed(3)} / 0.300</p>
          <span className="badge mt-2 bg-indigo-500/20 text-indigo-300">{metrics.embedding_drift.status}</span>
        </div>

        <div className="card p-4">
          <p className="text-sm text-slate-400">Prompt Input Validation</p>
          <p className="mt-2 text-3xl font-bold text-sky-300">
            {(metrics.prompt_input_validation.violation_rate * 100).toFixed(2)}%
          </p>
          <p className="mt-2 text-sm text-slate-300">Total records: {metrics.prompt_input_validation.total_records}</p>
          <p className="text-sm text-slate-300">Quarantined: {metrics.prompt_input_validation.quarantined}</p>
          <span className="badge mt-3 bg-sky-500/20 text-sky-300">{metrics.prompt_input_validation.status}</span>
        </div>

        <div className="card p-4">
          <p className="text-sm text-slate-400">LLM Output Schema</p>
          <p className="mt-2 text-3xl font-bold text-violet-300">
            {(metrics.llm_output_schema.violation_rate * 100).toFixed(2)}%
          </p>
          <p className="mt-2 text-sm text-slate-300">Total outputs: {metrics.llm_output_schema.total_outputs_checked}</p>
          <span
            className={`badge mt-3 ${
              metrics.llm_output_schema.trend === "RISING"
                ? "bg-red-500/20 text-red-300"
                : metrics.llm_output_schema.trend === "FALLING"
                  ? "bg-blue-500/20 text-blue-300"
                  : "bg-emerald-500/20 text-emerald-300"
            }`}
          >
            {metrics.llm_output_schema.trend}
          </span>
        </div>
      </div>
    </div>
  );
}
