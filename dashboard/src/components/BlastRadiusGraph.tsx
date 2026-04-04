import { Violation } from "../types";

interface BlastRadiusGraphProps {
  violation: Violation;
}

export function BlastRadiusGraph({ violation }: BlastRadiusGraphProps) {
  const pipelines = violation.blast_radius.affected_pipelines;
  const width = 380;
  const height = 220;
  const centerX = width / 2;
  const centerY = height / 2;
  const radius = 72;
  const mode = (violation.blast_radius.mode || "ENFORCE").toUpperCase();
  const nodeColor = mode === "ENFORCE" ? "#fb923c" : "#facc15";
  const centerSize = Math.max(14, Math.min(28, 14 + Math.sqrt(violation.records_failing || 1)));

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full rounded-lg bg-slate-900/70 p-2">
      {pipelines.map((pipeline, index) => {
        const angle = (index / Math.max(pipelines.length, 1)) * Math.PI * 2;
        const targetX = centerX + Math.cos(angle) * radius;
        const targetY = centerY + Math.sin(angle) * radius;
        const nodeSize = Math.max(8, Math.min(16, centerSize * 0.55));

        return (
          <g key={`${pipeline}-${index}`}>
            <line
              x1={centerX}
              y1={centerY}
              x2={targetX}
              y2={targetY}
              stroke="#475569"
              strokeWidth="1.2"
            />
            <text x={(centerX + targetX) / 2 + 4} y={(centerY + targetY) / 2 - 2} fill="#94a3b8" fontSize="8">
              {violation.failing_field}
            </text>
            <circle
              cx={targetX}
              cy={targetY}
              r={nodeSize}
              fill={nodeColor}
              className="animate-nodePop"
              style={{ animationDelay: `${index * 70}ms` }}
            />
            <text x={targetX} y={targetY + 2.5} fill="#111827" textAnchor="middle" fontSize="7" fontWeight="700">
              S
            </text>
            <text x={targetX} y={targetY + nodeSize + 12} fill="#cbd5e1" textAnchor="middle" fontSize="9">
              {pipeline}
            </text>
          </g>
        );
      })}

      <circle cx={centerX} cy={centerY} r={centerSize} fill="#ef4444" className="animate-nodePop" />
      <text x={centerX} y={centerY + 3} fill="#fff" textAnchor="middle" fontSize="9" fontWeight="700">
        Contract
      </text>
    </svg>
  );
}
