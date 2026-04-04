import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { EnforcementReportResponse } from "../types";

export function useEnforcementReport() {
  return useQuery({
    queryKey: ["enforcement-report"],
    queryFn: async () => {
      const { data } = await api.get<EnforcementReportResponse>("/enforcement-report");
      return data;
    },
    refetchInterval: 30_000
  });
}
