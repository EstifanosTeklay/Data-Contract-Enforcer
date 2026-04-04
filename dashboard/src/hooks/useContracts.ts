import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { ContractsResponse } from "../types";

export function useContracts() {
  return useQuery({
    queryKey: ["contracts"],
    queryFn: async () => {
      const { data } = await api.get<ContractsResponse>("/contracts");
      return data.items;
    },
    refetchInterval: 30_000
  });
}
