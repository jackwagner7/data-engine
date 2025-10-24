import express from "express";
import cors from "cors";
import multer from "multer";
import duckdb from "duckdb";
import fs from "fs";

const app = express();
app.use(cors());
app.use(express.json());

const upload = multer({ dest: "/data/uploads/" }); // persistent folder
const db = new duckdb.Database("/data/app.duckdb");

app.post("/upload", upload.single("file"), (req, res) => {
  const file = req.file.path;
  const table = "t_" + Date.now();
  db.run(
    `CREATE TABLE ${table} AS SELECT * FROM read_csv_auto('${file}', HEADER=TRUE);`,
    (err) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json({ table });
    }
  );
});

app.post("/query", (req, res) => {
  const { sql } = req.body;
  db.all(sql, (err, rows) => {
    if (err) return res.status(400).json({ error: err.message });

    // 🧠 Convert BigInt -> Number or String
    const safeRows = rows.map((row) => {
      const converted = {};
      for (const [key, value] of Object.entries(row)) {
        if (typeof value === "bigint") {
          // use Number if safe, else String to preserve value
          converted[key] =
            value < Number.MAX_SAFE_INTEGER ? Number(value) : value.toString();
        } else {
          converted[key] = value;
        }
      }
      return converted;
    });

    res.json({ rows: safeRows });
  });
});


app.listen(4000, () => console.log("✅ Data engine on port 4000"));
