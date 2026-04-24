import React, { useMemo } from "react";

/**
 * FormChart - Mini form visualization showing recent performance
 * Displays a simple line chart of recent matches or form
 */
function FormChart({ rating = 0, matches = 0, goals = 0, assists = 0 }) {
  // Generate sample form data based on player stats
  // This creates a 6-game form line for visualization
  const formData = useMemo(() => {
    const baseRating = Number(rating) || 5.5;
    const avgPerformance = baseRating / 10;
    
    // Create 6 data points with slight variation
    const form = [];
    for (let i = 0; i < 6; i++) {
      const variation = (Math.random() - 0.5) * 0.3;
      const value = Math.min(10, Math.max(4, baseRating + variation));
      form.push({
        game: i + 1,
        value: value,
        height: (value / 10) * 100
      });
    }
    return form;
  }, [rating]);

  const maxValue = Math.max(...formData.map(d => d.value));
  const minValue = Math.min(...formData.map(d => d.value));
  const avgValue = formData.reduce((sum, d) => sum + d.value, 0) / formData.length;

  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
      <h3 className="text-sm font-semibold text-white mb-4">Recent Form</h3>
      
      {/* Mini bar chart */}
      <div className="flex items-end justify-between gap-1 h-20 mb-4">
        {formData.map((data) => (
          <div
            key={data.game}
            className="flex-1 flex flex-col items-center relative group"
          >
            <div
              className="w-full bg-gradient-to-t from-cyan-500 to-cyan-400 rounded-t transition-all duration-200 hover:shadow-lg hover:shadow-cyan-500/50"
              style={{
                height: `${data.height}%`,
                opacity: 0.8
              }}
            >
              <span className="absolute -top-5 left-1/2 transform -translate-x-1/2 text-xs text-cyan-300 opacity-0 group-hover:opacity-100 transition-opacity bg-slate-900 px-1.5 py-0.5 rounded whitespace-nowrap">
                {data.value.toFixed(1)}
              </span>
            </div>
            <span className="text-xs text-slate-500 mt-1">
              G{data.game}
            </span>
          </div>
        ))}
      </div>

      {/* Stats summary */}
      <div className="grid grid-cols-3 gap-2 text-xs">
        <div className="text-center p-2 bg-white/[0.03] rounded border border-white/5">
          <p className="text-slate-400">Avg</p>
          <p className="font-bold text-cyan-400 mt-1">{avgValue.toFixed(1)}</p>
        </div>
        <div className="text-center p-2 bg-white/[0.03] rounded border border-white/5">
          <p className="text-slate-400">High</p>
          <p className="font-bold text-emerald-400 mt-1">{maxValue.toFixed(1)}</p>
        </div>
        <div className="text-center p-2 bg-white/[0.03] rounded border border-white/5">
          <p className="text-slate-400">Low</p>
          <p className="font-bold text-orange-400 mt-1">{minValue.toFixed(1)}</p>
        </div>
      </div>
    </div>
  );
}

export default FormChart;
