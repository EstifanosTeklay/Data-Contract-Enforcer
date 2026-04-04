import { ContractCards } from "../components/ContractCards";
import { useContracts } from "../hooks/useContracts";

export function Contracts() {
  const { data, isLoading } = useContracts();

  if (isLoading) {
    return (
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <div className="skeleton h-44" />
        <div className="skeleton h-44" />
        <div className="skeleton h-44" />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Contracts</h2>
      <ContractCards contracts={data || []} />
    </div>
  );
}
