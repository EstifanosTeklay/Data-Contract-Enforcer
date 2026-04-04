import { useMemo, useState } from "react";
import { Sidebar } from "./components/Sidebar";
import { AIMetrics } from "./pages/AIMetrics";
import { Contracts } from "./pages/Contracts";
import { EnforcementReport } from "./pages/EnforcementReport";
import { Overview } from "./pages/Overview";
import { Violations } from "./pages/Violations";
import { SectionKey } from "./types";

function renderSection(section: SectionKey) {
  switch (section) {
    case "overview":
      return <Overview />;
    case "violations":
      return <Violations />;
    case "ai":
      return <AIMetrics />;
    case "contracts":
      return <Contracts />;
    case "report":
      return <EnforcementReport />;
    default:
      return <Overview />;
  }
}

export default function App() {
  const [section, setSection] = useState<SectionKey>("overview");

  const title = useMemo(() => {
    if (section === "overview") return "Overview";
    if (section === "violations") return "Violations";
    if (section === "ai") return "AI Metrics";
    if (section === "report") return "Executive Briefing";
    return "Contracts";
  }, [section]);

  return (
    <div className="min-h-screen lg:flex bg-grid">
      <Sidebar active={section} onChange={setSection} />
      <main className="flex-1 p-4 md:p-6 lg:p-8">
        <header className="panel mb-6 flex flex-wrap items-center justify-between gap-4 p-4">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-indigo-300">Data Contract Enforcer Dashboard</p>
            <h2 className="mt-2 text-2xl font-bold text-white">{title}</h2>
          </div>
        </header>
        {renderSection(section)}
      </main>
    </div>
  );
}
