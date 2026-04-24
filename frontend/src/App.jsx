import { Navigate, Route, Routes } from "react-router-dom";
import { useEffect, useState } from "react";
import HomePage from "./pages/HomePage";
import PlayerListPage from "./pages/PlayerListPage";
import PlayerDetailPage from "./pages/PlayerDetailPage";
import CalendarPage from "./pages/CalendarPage";

function App() {
  const [theme, setTheme] = useState(() => localStorage.getItem("homestars-theme") || "dark");

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("homestars-theme", theme);
  }, [theme]);

  return (
    <div className="min-h-screen bg-pitch text-ink transition-colors duration-200">
      <button
        type="button"
        onClick={() => setTheme((prev) => (prev === "dark" ? "light" : "dark"))}
        className="fixed right-4 top-4 z-50 rounded-full border border-white/20 bg-white/10 px-3 py-1 text-xs font-semibold text-white backdrop-blur-sm transition-all duration-200 hover:border-stadium/70 hover:shadow-neon"
      >
        {theme === "dark" ? "Light Mode" : "Dark Mode"}
      </button>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/players/:country" element={<PlayerListPage />} />
        <Route path="/player/:id" element={<PlayerDetailPage />} />
        <Route path="/calendar" element={<CalendarPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  );
}

export default App;
