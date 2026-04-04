import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { AIMetricsResponse } from "../types";

export function useAIMetrics() {
  return useQuery({
    queryKey: ["ai-metrics"],
    queryFn: async () => {
      const { data } = await api.get<AIMetricsResponse>("/ai-metrics");
      return data;
    },
    refetchInterval: 30_000
  });
}
