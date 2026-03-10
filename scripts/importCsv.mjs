import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MongoClient } from "mongodb";
import { parse } from "csv-parse/sync";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvPath = process.argv[2] || path.join(__dirname, "sample-transcripts.csv");
const mongoUri = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const dbName = process.env.MONGODB_DB || "audio_search_demo";

if (!fs.existsSync(csvPath)) {
  console.error(`csv file not found: ${csvPath}`);
  process.exit(1);
}

const raw = fs.readFileSync(csvPath, "utf-8");
const rows = parse(raw, {
  columns: true,
  skip_empty_lines: true,
  trim: true,
});

if (!rows.length) {
  console.error("csv has no rows");
  process.exit(1);
}

const client = new MongoClient(mongoUri);
await client.connect();

const db = client.db(dbName);
const lectures = db.collection("lectures");
const transcripts = db.collection("transcripts");

const lectureDoc = {
  titleArabic: rows[0].titleArabic || "?????? ???????",
  audioUrl: rows[0].audioUrl || "https://example.com/sample.mp3",
  audioFileName: rows[0].audioFileName || "sample.mp3",
};

const lectureResult = await lectures.insertOne(lectureDoc);
const lectureId = lectureResult.insertedId;

const transcriptDocs = rows.map((row) => ({
  lectureId,
  text: row.text,
  startTimeSec: Number(row.startTimeSec || 0),
  speaker: row.speaker || "",
}));

await transcripts.insertMany(transcriptDocs);

console.log(`imported ${transcriptDocs.length} transcript rows`);
console.log(`lectureId: ${lectureId}`);

await client.close();