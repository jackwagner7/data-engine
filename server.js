import express from "express";
import cors from "cors";
import multer from "multer";
import duckdb from "duckdb";
import path from "path";
import fs from "fs";

const app = express();
const upload = multer({ dest: "uploads/" });

// one global DB + connection
const db = new duckdb.Database("data.db");
const conn = db.connect();

app.use(cors());
app.use(express.json());

function normalizeDisplayName(name) {
  const cleaned = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 30);
  if (cleaned.length > 0) return cleaned;
  return `table_${Date.now().toString(36)}`;
}

function generateTableId() {
  return `tbl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

// upload and create table
app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file uploaded" });

  const file = req.file.path;
  const originalName = path.basename(req.file.originalname, path.extname(req.file.originalname));
  const displayName = normalizeDisplayName(originalName);
  const tableId = generateTableId();

  const sql = `
    CREATE TABLE "${tableId}" AS
    SELECT * FROM read_csv_auto('${file}', HEADER=TRUE);
  `;

  conn.run(sql, (err) => {
    fs.unlink(file, () => {});
    if (err) return res.status(500).json({ error: err.message });
    console.log(`Table created: ${tableId} (alias: ${displayName})`);
    res.json({ tableId, displayName, sourceFilename: originalName });
  });
});

// drop table
app.delete("/upload/:tableId", (req, res) => {
  const { tableId } = req.params;
  if (!tableId) return res.status(400).json({ error: "Missing table id" });

  const sql = `DROP TABLE IF EXISTS "${tableId}"`;
  conn.run(sql, (err) => {
    if (err) {
      console.error("Failed to drop table:", err.message);
      return res.status(500).json({ error: err.message });
    }
    console.log(`Table dropped: ${tableId}`);
    res.json({ dropped: tableId });
  });
});

app.post("/query", (req, res) => {
  const { sql } = req.body;
  if (!sql) return res.status(400).json({ error: "Missing SQL" });

  console.log("Running SQL:", sql);

  try {
    conn.all(sql, (err, rows) => {
      if (err) {
        console.error("Query failed:", err.message);
        return res.status(400).json({ error: err.message });
      }

      const safeRows = rows.map((row) => {
        const out = {};
        for (const [key, val] of Object.entries(row)) {
          if (typeof val === "bigint") {
            const num = Number(val);
            out[key] = Number.isSafeInteger(num) ? num : val.toString();
          } else {
            out[key] = val;
          }
        }
        return out;
      });

      console.log(`Query succeeded (${safeRows.length} rows)`);
      res.json({ rows: safeRows });
    });
  } catch (e) {
    console.error("Unexpected error:", e);
    if (!res.headersSent) res.status(500).json({ error: e.message });
  }
});

// show tables (debug)
app.get("/tables", (_req, res) => {
  conn.all("SHOW TABLES;", (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(rows);
  });
});

// start server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  conn.all("SELECT version()", (_, version) => {
    if (version?.[0]) {
      console.log("DuckDB version:", version[0]);
    }
  });
  console.log(`Data engine running on port ${PORT}`);
});
