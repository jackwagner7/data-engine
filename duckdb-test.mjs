import duckdb from "duckdb";

console.log("🧠 Starting DuckDB direct test...");

const db = new duckdb.Database("data.db");
const conn = db.connect();

const sql = `
SELECT song_clean, COUNT(*) AS play_count
FROM classic_rock_raw_data3
GROUP BY song_clean
ORDER BY play_count DESC
LIMIT 1;
`;

try {
  console.log("▶ Running SQL...");
  conn.all(sql, (err, rows) => {
    if (err) {
      console.error("❌ Query failed:", err);
    } else {
      console.log("✅ Query succeeded:", rows);
    }

    // keep process alive a bit to see if it crashes later
    console.log("⏳ Waiting 5 seconds to check stability...");
    setTimeout(() => {
      console.log("✅ Still alive after query.");
      process.exit(0);
    }, 5000);
  });
} catch (err) {
  console.error("💥 Fatal DuckDB error:", err);
}
