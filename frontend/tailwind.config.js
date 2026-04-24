/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        pitch: "#0F172A",
        stadium: "#00FF41",
        panel: "#162033",
        ink: "#E2E8F0"
      },
      boxShadow: {
        neon: "0 0 24px rgba(0, 255, 65, 0.25)",
        soft: "0 12px 30px rgba(0, 0, 0, 0.35)"
      }
    }
  },
  plugins: []
};
