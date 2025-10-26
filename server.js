import express from "express";
import multer from "multer";
import cors from "cors";
import duckdb from "duckdb";
import path from "path";
import fs from "fs";

const app = express();
const upload = multer({ dest: "uploads/" });
const db = new duckdb.Database("data.db");

app.use(cors());
app.use(express.json());

// 🟢 Upload CSV and create table
app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file uploaded" });

  const file = req.file.path;
  const originalName = req.file.originalname || "dataset";

  // 🧹 Derive safe table name
  let baseName = path.basename(originalName, path.extname(originalName));
  baseName = baseName.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
  if (baseName.length > 30) baseName = baseName.slice(0, 30);

  const table = baseName;

  db.run(
    `CREATE OR REPLACE TABLE "${table}" AS SELECT * FROM read_csv_auto('${file}', HEADER=TRUE);`,
    (err) => {
      if (err) {
        console.error("❌ Table creation failed:", err.message);
        return res.status(500).json({ error: err.message });
      }
      console.log(`✅ Table created: ${table}`);
      res.json({ table });
    }
  );
});

// 🟢 Run SQL queries from frontend
app.post("/query", (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: "Missing SQL" });

  db.all(sql, (err, rows) => {
    if (err) {
      console.error("❌ Query failed:", err.message);
      return res.status(400).json({ error: err.message });
    }
    res.json({ rows });
  });
});

// 🟢 Start server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`✅ Data engine running on port ${PORT}`));
