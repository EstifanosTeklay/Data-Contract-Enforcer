interface LiveIndicatorProps {
  active: boolean;
}

export function LiveIndicator({ active }: LiveIndicatorProps) {
  return (
    <div className="flex items-center gap-2 rounded-full bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-300">
      <span className={`h-2.5 w-2.5 rounded-full ${active ? "bg-emerald-400 animate-pulseSoft" : "bg-slate-500"}`} />
      LIVE
    </div>
  );
}
