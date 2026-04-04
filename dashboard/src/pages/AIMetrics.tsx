import { AIMetricsCards } from "../components/AIMetricsCards";
import { useAIMetrics } from "../hooks/useAIMetrics";

export function AIMetrics() {
  const { data, isLoading } = useAIMetrics();

  if (isLoading || !data) {
    return (
      <div className="grid gap-4 lg:grid-cols-3">
        <div className="skeleton h-56" />
        <div className="skeleton h-56" />
        <div className="skeleton h-56" />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">AI Metrics</h2>
      <AIMetricsCards metrics={data} />
    </div>
  );
}
