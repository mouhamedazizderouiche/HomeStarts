require("dotenv").config();
const express = require("express");
const cors = require("cors");
const playerRoutes = require("./routes/playerRoutes");
const { connectDB } = require("./config/db");
const { hasSupabaseConfig } = require("./config/supabase");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 5000;

process.on("unhandledRejection", (err) => {
  console.error("UNHANDLED REJECTION:", err);
});

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

app.use(cors());
app.use(express.json());
// Serve local static photos if present (e.g. 'photo v0' folder)
app.use(
  "/static/photos",
  express.static(path.join(__dirname, "..", "photo v0"), { maxAge: "7d" })
);
app.use((req, res, next) => {
  const started = Date.now();
  res.on("finish", () => {
    console.log(req.method, req.path, res.statusCode, `${Date.now() - started}ms`);
  });
  next();
});

app.get("/", (_req, res) => {
  res.json({ message: "HomeStars API is running." });
});

app.use("/", playerRoutes);

if (hasSupabaseConfig()) {
  app.listen(PORT, () => {
    console.log(`HomeStars backend running on port ${PORT} (Supabase mode)`);
  });
} else {
  connectDB()
    .then(() => {
      app.listen(PORT, () => {
        console.log(`HomeStars backend running on port ${PORT} (Mongo mode)`);
      });
    })
    .catch((error) => {
      console.error("Database connection failed:", error.message);
      process.exit(1);
    });
}
