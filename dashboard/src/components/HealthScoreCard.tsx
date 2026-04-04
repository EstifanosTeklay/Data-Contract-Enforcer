interface HealthScoreCardProps {
  score: number;
}

function scoreColor(score: number) {
  if (score >= 80) return "text-emerald-400";
  if (score >= 60) return "text-yellow-400";
  return "text-red-400";
}

function scoreRing(score: number) {
  if (score >= 80) return "from-emerald-500/50 to-emerald-400/10";
  if (score >= 60) return "from-yellow-500/50 to-yellow-400/10";
  return "from-red-500/50 to-red-400/10";
}

export function HealthScoreCard({ score }: HealthScoreCardProps) {
  return (
    <div className="panel-gradient p-6 md:p-8 animate-fadeInUp">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-slate-400">Data Health Score</p>
          <p className={`mt-1 text-5xl font-extrabold ${scoreColor(score)}`}>{score}</p>
        </div>
        <div className={`h-24 w-24 rounded-full bg-gradient-to-br ${scoreRing(score)} p-1`}>
          <div className="flex h-full w-full items-center justify-center rounded-full bg-card text-sm text-slate-300">
            /100
          </div>
        </div>
      </div>
    </div>
  );
}
