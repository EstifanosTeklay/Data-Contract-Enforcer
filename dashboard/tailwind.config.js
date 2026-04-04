/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        base: "#0f1117",
        card: "#1a1d2e",
        sidebar: "#12151f",
        accent: "#6366f1"
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"]
      },
      keyframes: {
        fadeInUp: {
          "0%": { opacity: "0", transform: "translateY(8px)" },
          "100%": { opacity: "1", transform: "translateY(0)" }
        },
        pulseSoft: {
          "0%, 100%": { opacity: "0.55" },
          "50%": { opacity: "1" }
        },
        nodePop: {
          "0%": { transform: "scale(0.2)", opacity: "0" },
          "70%": { transform: "scale(1.08)", opacity: "1" },
          "100%": { transform: "scale(1)", opacity: "1" }
        }
      },
      animation: {
        fadeInUp: "fadeInUp 350ms ease-out",
        pulseSoft: "pulseSoft 1.2s ease-in-out infinite",
        nodePop: "nodePop 550ms cubic-bezier(0.2, 1.2, 0.2, 1)"
      }
    }
  },
  plugins: []
};
