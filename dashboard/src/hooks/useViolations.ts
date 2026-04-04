import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { ViolationsResponse } from "../types";

export function useViolations() {
  return useQuery({
    queryKey: ["violations"],
    queryFn: async () => {
      const { data } = await api.get<ViolationsResponse>("/violations");
      return data.items;
    },
    refetchInterval: 30_000
  });
}
