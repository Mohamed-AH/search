import fs from "node:fs";
import path from "node:path";
import { MongoClient, ObjectId } from "mongodb";
import { parse } from "csv-parse/sync";

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

function toBool(value, fallback) {
  if (value === undefined) return fallback;
  const normalized = String(value).trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(normalized);
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
  stem = stem.replace(/[_\-\s]+/g, "");
  stem = stem.replace(/[^\p{L}\p{N}]+/gu, "");
  return stem.toUpperCase();
}

function parseExportFile(exportPath) {
  const raw = fs.readFileSync(exportPath, "utf8");
  const lines = raw.split(/\r?\n/).map((line) => line.trim());

  const entries = [];
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

  function findMetadata(startIndex, maxDistance = 12) {
    for (let i = startIndex; i < lines.length && i - startIndex <= maxDistance; i += 1) {
      if (!lines[i].startsWith("Metadata:")) continue;
      const rawMeta = lines[i].slice("Metadata:".length).trim();
      try {
        return JSON.parse(rawMeta);
      } catch {
        return null;
      }
    }
    return null;
  }

  for (let i = 0; i < lines.length; i += 1) {
    if (!lines[i].startsWith("Audio:")) continue;

    const audioFileName = lines[i].slice("Audio:".length).trim();
    if (!audioFileName || seenAudio.has(audioFileName)) continue;

    const audioUrl = findForward(i, "Audio URL:");
    const shortIdRaw = findBackward(i, "Short ID:");
    const titleArabic = findBackward(i, "Title (AR):");
    const shortIdNum = Number(shortIdRaw);
    const metadata = findMetadata(i);
    const excelFileName = metadata?.excelFilename ? String(metadata.excelFilename).trim() : "";

    entries.push({
      audioFileName,
      audioUrl,
      excelFileName,
      shortId: Number.isFinite(shortIdNum) ? shortIdNum : null,
      titleArabic,
    });

    seenAudio.add(audioFileName);
  }

  const byStem = new Map();
  const byShortId = new Map();
  const duplicateStems = [];
  const duplicateShortIds = [];
  const missingShortId = [];
  const missingAudio = [];

  for (const entry of entries) {
    const hasAudio = Boolean(entry.audioFileName || entry.audioUrl);
    const hasShortId = Number.isFinite(entry.shortId);

    if (!hasShortId) missingShortId.push(entry);
    if (!hasAudio) missingAudio.push(entry);

    if (hasShortId) {
      const existingByShortId = byShortId.get(entry.shortId);
      if (!existingByShortId) {
        byShortId.set(entry.shortId, [entry]);
      } else {
        existingByShortId.push(entry);
        if (existingByShortId.length > 1) {
          duplicateShortIds.push({ shortId: entry.shortId, count: existingByShortId.length });
        }
      }
    }

    const stems = [
      normalizeStem(entry.audioFileName),
      normalizeStem(entry.audioUrl),
      normalizeStem(entry.excelFileName),
    ].filter(Boolean);

    for (const stem of stems) {
      const existing = byStem.get(stem);
      const signature = `${entry.shortId ?? ""}|${entry.audioFileName ?? ""}|${entry.audioUrl ?? ""}|${entry.excelFileName ?? ""}`;

      if (!existing) {
        byStem.set(stem, [entry]);
        continue;
      }

      const alreadyThere = existing.some((x) => `${x.shortId ?? ""}|${x.audioFileName ?? ""}|${x.audioUrl ?? ""}|${x.excelFileName ?? ""}` === signature);
      if (alreadyThere) continue;

      existing.push(entry);
      if (existing.length > 1) {
        duplicateStems.push({ stem, shortIds: existing.map((x) => x.shortId).filter((x) => x !== null) });
      }
    }
  }

  return {
    entries,
    byStem,
    byShortId,
    issues: { duplicateStems, duplicateShortIds, missingShortId, missingAudio },
  };
}

function parseTranscriptCsv(csvPath) {
  const raw = fs.readFileSync(csvPath, "utf8");
  const rows = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
  });

  return rows
    .map((row, idx) => {
      const startMs = Number(row.start ?? row.startTimeMs ?? row.startTime ?? 0);
      const endMs = Number(row.end ?? row.endTimeMs ?? row.endTime ?? 0);
      const text = String(row.text ?? "").trim();

      return {
        lineNumber: idx + 1,
        text,
        speaker: row.speaker ? String(row.speaker).trim() : "",
        startTimeMs: Number.isFinite(startMs) ? startMs : 0,
        startTimeSec: Number.isFinite(startMs) ? startMs / 1000 : 0,
        endTimeMs: Number.isFinite(endMs) ? endMs : 0,
      };
    })
    .filter((row) => row.text.length > 0);
}

function loadCheckpoint(checkpointPath, reset) {
  if (reset || !fs.existsSync(checkpointPath)) {
    return {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      processed: {},
    };
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(checkpointPath, "utf8"));
    if (!parsed.processed || typeof parsed.processed !== "object") {
      parsed.processed = {};
    }
    return parsed;
  } catch {
    return {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      processed: {},
      warning: "checkpoint was unreadable and got reset",
    };
  }
}

function saveCheckpoint(checkpointPath, checkpoint) {
  checkpoint.updatedAt = new Date().toISOString();
  fs.writeFileSync(checkpointPath, JSON.stringify(checkpoint, null, 2), "utf8");
}

async function safeCreateIndex(collection, key, options = {}) {
  try {
    await collection.createIndex(key, options);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const existsConflict =
      message.includes("existing index has the same name") ||
      message.includes("already exists with different options");
    if (!existsConflict) throw error;
  }
}

async function ensureCollections(db) {
  const collections = await db.listCollections().toArray();
  const names = new Set(collections.map((c) => c.name));

  if (!names.has("lectures")) await db.createCollection("lectures");
  if (!names.has("transcripts")) await db.createCollection("transcripts");

  const lectures = db.collection("lectures");
  const transcripts = db.collection("transcripts");

  await safeCreateIndex(lectures, { shortId: 1 });
  await safeCreateIndex(lectures, { audioFileName: 1 });
  await safeCreateIndex(transcripts, { lectureId: 1, startTimeSec: 1 });
  await safeCreateIndex(transcripts, { shortId: 1 });
}

async function detectDbDuplicateShortIds(db) {
  const duplicates = await db
    .collection("lectures")
    .aggregate([
      { $match: { shortId: { $type: "number" } } },
      {
        $group: {
          _id: "$shortId",
          count: { $sum: 1 },
          ids: { $push: "$_id" },
        },
      },
      { $match: { count: { $gt: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 500 },
    ])
    .toArray();

  return duplicates.map((d) => ({
    shortId: d._id,
    count: d.count,
    ids: d.ids.map((x) => String(x)),
  }));
}

async function upsertLectureAndTranscripts(db, lectureMeta, transcriptRows, options) {
  const lectures = db.collection("lectures");
  const transcripts = db.collection("transcripts");

  const lectureDoc = {
    shortId: lectureMeta.shortId,
    titleArabic: lectureMeta.titleArabic || lectureMeta.baseName,
    audioUrl: lectureMeta.audioUrl || "",
    audioFileName: lectureMeta.audioFileName || `${lectureMeta.baseName}.m4a`,
    sourceCsv: lectureMeta.csvFileName,
    updatedAt: new Date(),
  };

  const existing = await lectures
    .find({ shortId: lectureMeta.shortId })
    .sort({ updatedAt: -1, _id: 1 })
    .toArray();

  let lectureId;
  let duplicateShortIdInDb = false;

  if (existing.length === 0) {
    const ins = await lectures.insertOne({ ...lectureDoc, createdAt: new Date() });
    lectureId = ins.insertedId;
  } else {
    duplicateShortIdInDb = existing.length > 1;
    lectureId = existing[0]._id;
    await lectures.updateOne({ _id: lectureId }, { $set: lectureDoc });
  }

  if (options.replaceTranscripts) {
    await transcripts.deleteMany({ lectureId });
  }

  if (transcriptRows.length) {
    const docs = transcriptRows.map((row) => ({
      lectureId,
      shortId: lectureMeta.shortId,
      text: row.text,
      speaker: row.speaker,
      startTimeSec: row.startTimeSec,
      startTimeMs: row.startTimeMs,
      endTimeMs: row.endTimeMs,
      sourceCsv: lectureMeta.csvFileName,
    }));

    const batchSize = 1000;
    for (let i = 0; i < docs.length; i += batchSize) {
      const batch = docs.slice(i, i + batchSize);
      await transcripts.insertMany(batch, { ordered: false });
    }
  }

  return {
    lectureId: String(lectureId),
    shortId: lectureMeta.shortId,
    transcriptsInserted: transcriptRows.length,
    csvFileName: lectureMeta.csvFileName,
    duplicateShortIdInDb,
  };
}

async function getMongoUsage(db, client) {
  const usage = {
    dbStats: null,
    serverMemory: null,
  };

  try {
    const stats = await db.command({ dbStats: 1 });
    usage.dbStats = {
      dataSizeBytes: stats.dataSize,
      storageSizeBytes: stats.storageSize,
      indexSizeBytes: stats.indexSize,
      collections: stats.collections,
      objects: stats.objects,
    };
  } catch (error) {
    usage.dbStats = { error: error instanceof Error ? error.message : String(error) };
  }

  try {
    const serverStatus = await client.db("admin").command({ serverStatus: 1 });
    if (serverStatus?.mem) {
      usage.serverMemory = {
        residentMB: serverStatus.mem.resident,
        virtualMB: serverStatus.mem.virtual,
        mappedMB: serverStatus.mem.mapped,
      };
    }
  } catch (error) {
    usage.serverMemory = { note: "serverStatus not available", detail: error instanceof Error ? error.message : String(error) };
  }

  return usage;
}

function pickBestCandidate(candidates, baseName) {
  if (!candidates || candidates.length === 0) return { candidate: null, reason: "no-candidate" };

  const withShortId = candidates.filter((c) => Number.isFinite(c.shortId));
  const withAudio = withShortId.filter((c) => c.audioFileName || c.audioUrl);
  const targetStem = normalizeStem(baseName);
  const exactStem = withAudio.filter((c) => {
    const candidateStems = [
      normalizeStem(c.audioFileName || ""),
      normalizeStem(c.audioUrl || ""),
      normalizeStem(c.excelFileName || ""),
    ].filter(Boolean);
    return candidateStems.includes(targetStem);
  });

  if (exactStem.length === 1) return { candidate: exactStem[0], reason: "exact" };
  if (exactStem.length > 1) return { candidate: null, reason: "ambiguous-exact" };
  if (withAudio.length === 1) return { candidate: withAudio[0], reason: "single-valid" };
  if (withAudio.length > 1) return { candidate: null, reason: "ambiguous" };
  if (withShortId.length > 0) return { candidate: null, reason: "missing-audio" };

  return { candidate: null, reason: "missing-shortid" };
}

async function main() {
  const args = parseArgs(process.argv);
  const rootDir = process.cwd();

  const options = {
    exportPath: path.resolve(rootDir, args.export || "sample-app/data-export.txt"),
    csvDir: path.resolve(rootDir, args.dir || "sample-app/transcripts"),
    singleFile: args.file ? path.resolve(rootDir, args.file) : null,
    importAll: Boolean(args.all),
    dryRun: Boolean(args["dry-run"]),
    replaceTranscripts: toBool(args.replace, true),
    resume: toBool(args.resume, true),
    resetCheckpoint: Boolean(args["reset-checkpoint"]),
    checkpointPath: path.resolve(rootDir, args.checkpoint || "sample-app/scripts/import-checkpoint.json"),
    reportPath: path.resolve(rootDir, args.report || "sample-app/scripts/import-report.json"),
    minLines: toNumber(args["min-lines"], 20),
    stopOnError: toBool(args["stop-on-error"], false),
  };

  if (!options.singleFile && !options.importAll) {
    console.error("use either --file <path-to-csv> or --all");
    process.exit(1);
  }

  if (!fs.existsSync(options.exportPath)) {
    console.error(`export file not found: ${options.exportPath}`);
    process.exit(1);
  }

  if (!options.singleFile && !fs.existsSync(options.csvDir)) {
    console.error(`transcripts directory not found: ${options.csvDir}`);
    process.exit(1);
  }

  const report = {
    runAt: new Date().toISOString(),
    options: {
      exportPath: options.exportPath,
      csvDir: options.csvDir,
      singleFile: options.singleFile,
      dryRun: options.dryRun,
      replaceTranscripts: options.replaceTranscripts,
      resume: options.resume,
      minLines: options.minLines,
    },
    issues: {
      exportDuplicateStems: [],
      exportDuplicateShortIds: [],
      exportMissingShortId: [],
      exportMissingAudio: [],
      duplicateTranscriptStemFiles: [],
      duplicateShortIdAssignments: [],
      missingMapping: [],
      missingShortIdForMatch: [],
      missingAudioForMatch: [],
      ambiguousMatches: [],
      lowLineCount: [],
      writeFailures: [],
      dbDuplicateShortIds: [],
    },
    progress: {
      totalCsvDiscovered: 0,
      matchedForImport: 0,
      skippedByResume: 0,
      importedOk: 0,
      failed: 0,
      transcriptsInserted: 0,
      unmatched: 0,
    },
    imported: [],
    skipped: [],
    mongoUsage: null,
    checkpointPath: options.checkpointPath,
  };

  const exportData = parseExportFile(options.exportPath);
  report.issues.exportDuplicateStems = exportData.issues.duplicateStems;
  report.issues.exportDuplicateShortIds = exportData.issues.duplicateShortIds;
  report.issues.exportMissingShortId = exportData.issues.missingShortId.map((x) => ({
    titleArabic: x.titleArabic || "",
    audioFileName: x.audioFileName || "",
  }));
  report.issues.exportMissingAudio = exportData.issues.missingAudio.map((x) => ({
    titleArabic: x.titleArabic || "",
    shortId: x.shortId ?? null,
  }));

  const csvFiles = options.singleFile
    ? [options.singleFile]
    : fs
        .readdirSync(options.csvDir)
        .filter((name) => name.toLowerCase().endsWith(".csv"))
        .map((name) => path.join(options.csvDir, name));

  report.progress.totalCsvDiscovered = csvFiles.length;

  const mapped = [];

  for (const csvPath of csvFiles) {
    const csvFileName = path.basename(csvPath);
    const baseName = csvFileName.replace(/\.csv$/i, "");
    const shortId = Number(baseName);

    if (!Number.isFinite(shortId)) {
      report.issues.missingShortIdForMatch.push(csvFileName);
      continue;
    }

    const candidates = exportData.byShortId.get(shortId) || [];
    if (candidates.length === 0) {
      report.issues.missingMapping.push(csvFileName);
      continue;
    }
    if (candidates.length > 1) {
      report.issues.ambiguousMatches.push({
        csvFileName,
        reason: "duplicate-shortid",
        candidates: candidates.map((x) => ({ shortId: x.shortId ?? null, audioFileName: x.audioFileName || "" })),
      });
      continue;
    }

    const candidate = candidates[0];

    mapped.push({
      csvPath,
      csvFileName,
      baseName,
      shortId: candidate.shortId,
      titleArabic: candidate.titleArabic || baseName,
      audioFileName: candidate.audioFileName || `${baseName}.m4a`,
      audioUrl: candidate.audioUrl || "",
    });
  }

  const shortIdMap = new Map();
  for (const item of mapped) {
    if (!shortIdMap.has(item.shortId)) shortIdMap.set(item.shortId, []);
    shortIdMap.get(item.shortId).push(item.csvFileName);
  }

  const dedupedMapped = [];
  for (const item of mapped) {
    const files = shortIdMap.get(item.shortId);
    if (files.length > 1 && files[0] !== item.csvFileName) {
      report.skipped.push({ csvFileName: item.csvFileName, reason: `duplicate-shortId-${item.shortId}` });
      continue;
    }
    dedupedMapped.push(item);
  }

  for (const [shortId, files] of shortIdMap.entries()) {
    if (files.length > 1) {
      report.issues.duplicateShortIdAssignments.push({ shortId, files });
    }
  }

  report.progress.unmatched =
    report.issues.missingMapping.length +
    report.issues.missingShortIdForMatch.length +
    report.issues.missingAudioForMatch.length +
    report.issues.ambiguousMatches.length;

  const checkpoint = loadCheckpoint(options.checkpointPath, options.resetCheckpoint);

  const queue = [];
  for (const item of dedupedMapped) {
    if (options.resume && checkpoint.processed[item.csvFileName]?.status === "success") {
      report.progress.skippedByResume += 1;
      report.skipped.push({ csvFileName: item.csvFileName, reason: "resume-skip-success" });
      continue;
    }
    queue.push(item);
  }

  report.progress.matchedForImport = queue.length;

  if (options.dryRun) {
    console.log(`dry run: discovered=${report.progress.totalCsvDiscovered}, queued=${queue.length}, resumeSkipped=${report.progress.skippedByResume}`);
    console.log(`issues: unmatched=${report.progress.unmatched}, dupStemFiles=${report.issues.duplicateTranscriptStemFiles.length}, dupShortIdAssignments=${report.issues.duplicateShortIdAssignments.length}`);
    for (const item of queue.slice(0, 20)) {
      console.log(`${item.csvFileName} -> shortId ${item.shortId} -> ${item.audioFileName}`);
    }
    fs.writeFileSync(options.reportPath, JSON.stringify(report, null, 2), "utf8");
    process.exit(0);
  }

  const mongoUri = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
  const dbName = process.env.MONGODB_DB || "audio_search_demo";

  const client = new MongoClient(mongoUri);
  await client.connect();
  const db = client.db(dbName);

  try {
    await ensureCollections(db);
    report.issues.dbDuplicateShortIds = await detectDbDuplicateShortIds(db);

    for (const item of queue) {
      const startedAt = new Date().toISOString();

      try {
        const rows = parseTranscriptCsv(item.csvPath);

        if (rows.length < options.minLines) {
          report.issues.lowLineCount.push({ csvFileName: item.csvFileName, shortId: item.shortId, lineCount: rows.length });
        }

        const saved = await upsertLectureAndTranscripts(db, item, rows, {
          replaceTranscripts: options.replaceTranscripts,
        });

        report.imported.push({
          csvFileName: saved.csvFileName,
          shortId: saved.shortId,
          lectureId: saved.lectureId,
          transcriptsInserted: saved.transcriptsInserted,
          duplicateShortIdInDb: saved.duplicateShortIdInDb,
        });

        report.progress.importedOk += 1;
        report.progress.transcriptsInserted += saved.transcriptsInserted;

        checkpoint.processed[item.csvFileName] = {
          status: "success",
          shortId: item.shortId,
          transcriptsInserted: saved.transcriptsInserted,
          startedAt,
          finishedAt: new Date().toISOString(),
        };

        saveCheckpoint(options.checkpointPath, checkpoint);
        console.log(`imported ${item.csvFileName}: shortId=${saved.shortId}, transcripts=${saved.transcriptsInserted}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);

        report.progress.failed += 1;
        report.issues.writeFailures.push({
          csvFileName: item.csvFileName,
          shortId: item.shortId,
          error: message,
        });

        checkpoint.processed[item.csvFileName] = {
          status: "failed",
          shortId: item.shortId,
          startedAt,
          finishedAt: new Date().toISOString(),
          error: message,
        };

        saveCheckpoint(options.checkpointPath, checkpoint);
        console.error(`failed ${item.csvFileName}: ${message}`);

        if (options.stopOnError) {
          throw error;
        }
      }
    }

    report.mongoUsage = await getMongoUsage(db, client);
  } finally {
    await client.close();
  }

  fs.writeFileSync(options.reportPath, JSON.stringify(report, null, 2), "utf8");

  console.log("\nimport complete");
  console.log(`report: ${options.reportPath}`);
  console.log(`checkpoint: ${options.checkpointPath}`);
  console.log(`discovered=${report.progress.totalCsvDiscovered}, queued=${report.progress.matchedForImport}, success=${report.progress.importedOk}, failed=${report.progress.failed}, resumeSkipped=${report.progress.skippedByResume}`);
  console.log(`transcripts inserted=${report.progress.transcriptsInserted}`);
  console.log(`unmatched=${report.progress.unmatched}, lowLineCount=${report.issues.lowLineCount.length}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});