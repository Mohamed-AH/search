import fs from "node:fs";
import path from "node:path";
import { MongoClient } from "mongodb";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;

    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
    } else {
      args[key] = next;
      i += 1;
    }
  }
  return args;
}

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeStem(value) {
  if (!value) return "";

  const decoded = (() => {
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  })();

  let stem = path.basename(decoded).replace(/\.[^.]+$/, "");
  stem = stem.replace(/^Ringtone-/i, "");
  stem = stem.replace(/-\d{10,}-\d+$/u, "");
  stem = stem.replace(/\s+\(\d+\)$/u, "");
  return stem.toUpperCase();
}

function parseExportFile(exportPath) {
  const raw = fs.readFileSync(exportPath, "utf8");
  const lines = raw.split(/\r?\n/).map((line) => line.trim());

  const lectures = [];
  const seenAudio = new Set();

  function findBackward(startIndex, prefix, maxDistance = 120) {
    for (let i = startIndex; i >= 0 && startIndex - i <= maxDistance; i -= 1) {
      if (lines[i].startsWith(prefix)) return lines[i].slice(prefix.length).trim();
    }
    return "";
  }

  function findForward(startIndex, prefix, maxDistance = 10) {
    for (let i = startIndex; i < lines.length && i - startIndex <= maxDistance; i += 1) {
      if (lines[i].startsWith(prefix)) return lines[i].slice(prefix.length).trim();
    }
    return "";
  }

  for (let i = 0; i < lines.length; i += 1) {
    if (!lines[i].startsWith("Audio:")) continue;

    const audioFileName = lines[i].slice("Audio:".length).trim();
    if (!audioFileName || seenAudio.has(audioFileName)) continue;

    const audioUrl = findForward(i, "Audio URL:");
    const shortIdRaw = findBackward(i, "Short ID:");
    const titleArabic = findBackward(i, "Title (AR):");
    const shortIdNum = Number(shortIdRaw);

    lectures.push({
      shortId: Number.isFinite(shortIdNum) ? shortIdNum : null,
      titleArabic: titleArabic || "",
      audioFileName,
      audioUrl,
      audioStem: normalizeStem(audioFileName || audioUrl),
    });

    seenAudio.add(audioFileName);
  }

  return lectures;
}

function loadCheckpoint(checkpointPath) {
  if (!checkpointPath || !fs.existsSync(checkpointPath)) return { processed: {} };
  try {
    return JSON.parse(fs.readFileSync(checkpointPath, "utf8"));
  } catch {
    return { processed: {} };
  }
}

function toCsv(rows) {
  if (!rows.length) return "";

  const headers = Object.keys(rows[0]);
  const escape = (value) => {
    const str = value === null || value === undefined ? "" : String(value);
    if (str.includes('"') || str.includes(",") || str.includes("\n")) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => escape(row[h])).join(","));
  }
  return lines.join("\n");
}

async function main() {
  const args = parseArgs(process.argv);
  const rootDir = process.cwd();

  const exportPath = path.resolve(rootDir, args.export || "sample-app/data-export.txt");
  const checkpointPath = path.resolve(rootDir, args.checkpoint || "sample-app/scripts/import-checkpoint.json");
  const reportJsonPath = path.resolve(rootDir, args.out || "sample-app/reports/master-coverage-report.json");
  const reportCsvPath = path.resolve(rootDir, args.outcsv || "sample-app/reports/master-coverage-report.csv");
  const minLines = toNumber(args["min-lines"], 20);

  if (!fs.existsSync(exportPath)) {
    console.error(`export file not found: ${exportPath}`);
    process.exit(1);
  }

  const lecturesFromExport = parseExportFile(exportPath);
  const checkpoint = loadCheckpoint(checkpointPath);

  const mongoUri = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
  const dbName = process.env.MONGODB_DB || "audio_search_demo";

  const client = new MongoClient(mongoUri);
  await client.connect();
  const db = client.db(dbName);

  const [lectureCounts, transcriptCounts, dbLectureDuplicates, orphanTranscripts] = await Promise.all([
    db
      .collection("lectures")
      .aggregate([
        { $match: { shortId: { $type: "number" } } },
        { $group: { _id: "$shortId", count: { $sum: 1 } } },
      ])
      .toArray(),
    db
      .collection("transcripts")
      .aggregate([
        { $match: { shortId: { $type: "number" } } },
        { $group: { _id: "$shortId", lineCount: { $sum: 1 } } },
      ])
      .toArray(),
    db
      .collection("lectures")
      .aggregate([
        { $match: { shortId: { $type: "number" } } },
        { $group: { _id: "$shortId", count: { $sum: 1 }, ids: { $push: "$_id" } } },
        { $match: { count: { $gt: 1 } } },
      ])
      .toArray(),
    db
      .collection("transcripts")
      .aggregate([
        { $match: { shortId: { $type: "number" } } },
        { $group: { _id: "$shortId", lineCount: { $sum: 1 } } },
        {
          $lookup: {
            from: "lectures",
            let: { shortId: "$_id" },
            pipeline: [
              { $match: { $expr: { $eq: ["$shortId", "$$shortId"] } } },
              { $limit: 1 },
            ],
            as: "lecture",
          },
        },
        { $match: { lecture: { $size: 0 } } },
      ])
      .toArray(),
  ]);

  await client.close();

  const lectureCountByShortId = new Map(lectureCounts.map((x) => [x._id, x.count]));
  const transcriptCountByShortId = new Map(transcriptCounts.map((x) => [x._id, x.lineCount]));
  const duplicatesByShortId = new Map(dbLectureDuplicates.map((x) => [x._id, x]));

  const rows = [];

  for (const lecture of lecturesFromExport) {
    const shortId = lecture.shortId;
    const lineCount = Number.isFinite(shortId) ? transcriptCountByShortId.get(shortId) || 0 : 0;
    const lectureDocCount = Number.isFinite(shortId) ? lectureCountByShortId.get(shortId) || 0 : 0;

    const pendingActions = [];
    let status = "needs_transcription";

    if (!Number.isFinite(shortId)) {
      status = "invalid_missing_shortid";
      pendingActions.push("fix_shortid_in_source");
    } else if (lineCount >= minLines) {
      status = "covered";
    } else if (lineCount > 0 && lineCount < minLines) {
      status = "low_lines_review";
      pendingActions.push("verify_transcript_quality");
    } else {
      pendingActions.push("transcription_needed");
    }

    if (!lecture.audioUrl) {
      pendingActions.push("missing_audio_url");
    }

    if (lectureDocCount === 0) {
      pendingActions.push("lecture_doc_missing_in_db");
    }

    if (duplicatesByShortId.has(shortId)) {
      pendingActions.push("dedupe_shortid_records");
    }

    const checkpointRecord = checkpoint.processed?.[`${lecture.audioStem}.csv`] || null;
    if (checkpointRecord?.status === "failed") {
      pendingActions.push("import_failed_retry");
    }

    rows.push({
      shortId: Number.isFinite(shortId) ? shortId : "",
      status,
      lineCount,
      titleArabic: lecture.titleArabic,
      audioFileName: lecture.audioFileName,
      audioUrl: lecture.audioUrl,
      lectureDocCount,
      pendingActions: pendingActions.join(";"),
    });
  }

  rows.sort((a, b) => Number(a.shortId || 0) - Number(b.shortId || 0));

  const summary = {
    totalLecturesInExport: rows.length,
    covered: rows.filter((x) => x.status === "covered").length,
    lowLinesReview: rows.filter((x) => x.status === "low_lines_review").length,
    needsTranscription: rows.filter((x) => x.status === "needs_transcription").length,
    invalidMissingShortId: rows.filter((x) => x.status === "invalid_missing_shortid").length,
    dbDuplicateShortIds: dbLectureDuplicates.length,
    orphanTranscriptShortIds: orphanTranscripts.map((x) => x._id),
    minLinesThreshold: minLines,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    summary,
    rows,
  };

  fs.mkdirSync(path.dirname(reportJsonPath), { recursive: true });
  fs.mkdirSync(path.dirname(reportCsvPath), { recursive: true });

  fs.writeFileSync(reportJsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(reportCsvPath, toCsv(rows), "utf8");

  console.log(`master report written:`);
  console.log(`json: ${reportJsonPath}`);
  console.log(`csv: ${reportCsvPath}`);
  console.log(`summary: covered=${summary.covered}, lowLines=${summary.lowLinesReview}, needsTranscription=${summary.needsTranscription}, total=${summary.totalLecturesInExport}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});