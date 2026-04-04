import { SectionKey } from "../types";

interface SidebarProps {
  active: SectionKey;
  onChange: (section: SectionKey) => void;
}

const navItems: Array<{
  key: SectionKey;
  label: string;
  short: string;
  helper: string;
  tone: "indigo" | "red" | "emerald" | "sky" | "amber";
}> = [
  { key: "overview", label: "Overview", short: "OV", helper: "Health and status", tone: "indigo" },
  { key: "report", label: "Executive Briefing", short: "EX", helper: "Stakeholder narrative", tone: "amber" },
  { key: "violations", label: "Violations", short: "AL", helper: "Live incidents", tone: "red" },
  { key: "ai", label: "AI Metrics", short: "AI", helper: "Model risk posture", tone: "emerald" },
  { key: "contracts", label: "Contracts", short: "CT", helper: "Rules and owners", tone: "sky" }
];

export function Sidebar({ active, onChange }: SidebarProps) {
  return (
    <aside className="w-full lg:w-80 bg-sidebar/95 backdrop-blur-md border-r border-slate-800/80 p-4 lg:p-6">
      <div className="nav-shell mb-8">
        <p className="text-xs uppercase tracking-[0.22em] text-indigo-300">Data Platform</p>
        <h1 className="mt-2 text-2xl font-bold text-white">Contract Command Center</h1>
        <p className="mt-2 text-xs text-slate-400">Track reliability signals, explain risk, and brief stakeholders with confidence.</p>
      </div>

      <p className="mb-3 text-[11px] uppercase tracking-[0.16em] text-slate-500">Workspace</p>
      <nav className="space-y-2.5">
        {navItems.map((item) => {
          const isActive = active === item.key;
          const iconClass = `nav-icon nav-icon-${item.tone} ${isActive ? "nav-icon-active" : ""}`;

          return (
            <button
              key={item.key}
              className={`nav-item ${isActive ? "nav-item-active" : ""}`}
              onClick={() => onChange(item.key)}
            >
              <span className={iconClass} aria-hidden="true">{item.short}</span>
              <span className="min-w-0 flex-1 text-left">
                <span className="block text-sm font-semibold">{item.label}</span>
                <span className={`block text-xs ${isActive ? "text-slate-200/95" : "text-slate-400"}`}>{item.helper}</span>
              </span>
              <span className={`nav-chevron ${isActive ? "nav-chevron-active" : ""}`}>›</span>
            </button>
          );
        })}
      </nav>
    </aside>
  );
}
