import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { HealthResponse } from "../types";

export function useHealthData() {
  return useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const { data } = await api.get<HealthResponse>("/health");
      return data;
    },
    refetchInterval: 30_000
  });
}
