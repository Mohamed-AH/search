import dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MongoClient } from "mongodb";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = Number(process.env.PORT || 4000);
const mongoUri = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const dbName = process.env.MONGODB_DB || "audio_search_demo";
const searchMode = process.env.SEARCH_MODE || "local"; // local | atlas

let db;
let indexesReady = false;
const CONTEXT_WINDOW_SEC = Number(process.env.CONTEXT_WINDOW_SEC || 90);
const CONTEXT_ITEMS = Number(process.env.CONTEXT_ITEMS || 2);

async function getDb() {
  if (db) return db;

  const client = new MongoClient(mongoUri);
  await client.connect();
  db = client.db(dbName);
  return db;
}

async function ensureLocalIndexes(database) {
  if (indexesReady || searchMode !== "local") return;
  await database.collection("transcripts").createIndex({ text: "text" });
  indexesReady = true;
}

function joinedProjection() {
  return [
    {
      $lookup: {
        from: "lectures",
        localField: "lectureId",
        foreignField: "_id",
        as: "lecture",
      },
    },
    { $unwind: "$lecture" },
    {
      $project: {
        _id: 1,
        lectureId: 1,
        text: 1,
        startTimeSec: 1,
        speaker: 1,
        lectureTitleArabic: "$lecture.titleArabic",
        audioUrl: "$lecture.audioUrl",
        audioFileName: "$lecture.audioFileName",
      },
    },
    { $limit: 25 },
  ];
}

function normalizeStartRange(value, windowSeconds) {
  const raw = Number(value || 0);
  if (!Number.isFinite(raw)) return { min: 0, max: 0, raw: 0 };
  if (raw > 100000) {
    const windowMs = Math.max(0, windowSeconds) * 1000;
    return { min: raw - windowMs, max: raw + windowMs, raw };
  }
  const window = Math.max(0, windowSeconds);
  return { min: raw - window, max: raw + window, raw };
}

async function enrichResultsWithContext(results, transcripts) {
  if (!Array.isArray(results) || results.length === 0) return results;

  return Promise.all(
    results.map(async (result) => {
      if (!result?.lectureId || result.startTimeSec == null) {
        return { ...result, contextBefore: [], contextAfter: [] };
      }

      const { min, max, raw } = normalizeStartRange(result.startTimeSec, CONTEXT_WINDOW_SEC);
      const nearby = await transcripts
        .find({
          lectureId: result.lectureId,
          startTimeSec: { $gte: min, $lte: max },
        })
        .project({ _id: 1, text: 1, startTimeSec: 1 })
        .sort({ startTimeSec: 1 })
        .toArray();

      const filtered = nearby.filter((item) => String(item._id) !== String(result._id));
      const before = filtered.filter((item) => item.startTimeSec < raw);
      const after = filtered.filter((item) => item.startTimeSec > raw);

      return {
        ...result,
        contextBefore: before.slice(-CONTEXT_ITEMS),
        contextAfter: after.slice(0, CONTEXT_ITEMS),
      };
    })
  );
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function toArabicFlexibleRegex(value) {
  return escapeRegex(String(value || ""))
    .replace(/ا/g, "[اأإآٱ]")
    .replace(/ى/g, "[ىي]")
    .replace(/ي/g, "[يى]")
    .replace(/ة/g, "[هة]")
    .replace(/ه/g, "[هة]");
}

function normalizeArabic(text) {
  return String(text || "")
    .normalize("NFKC")
    .replace(/[\u064B-\u065F\u0670]/g, "")
    .replace(/ـ/g, "")
    .replace(/[أإآٱ]/g, "ا")
    .replace(/ى/g, "ي")
    .replace(/ؤ/g, "و")
    .replace(/ئ/g, "ي")
    .replace(/ة/g, "ه")
    .replace(/\s+/g, " ")
    .trim();
}

const WORD_FIXES = new Map([
  ["اشيخ", "الشيخ"],
  ["النجمين", "النجمي"],
  ["يحياء", "يحيى"],
]);

const KNOWN_ALIAS_GROUPS = [
  [
    "أحمد النجمي",
    "احمد النجمي",
    "الشيخ أحمد النجمي",
    "الشيخ احمد النجمي",
    "اشيخ احمد النجمين",
    "الشيخ احمد بن يحي النجمي",
    "الشيخ احمد بن يحيى النجمي",
    "الشيخ احمد بن يحي النجم",
  ],
];

function correctedWordsVariant(text) {
  const words = normalizeArabic(text).split(" ").filter(Boolean);
  if (words.length === 0) return "";
  return words.map((word) => WORD_FIXES.get(word) || word).join(" ");
}

function stripSheikhTitle(text) {
  return normalizeArabic(text).replace(/^(الشيخ|شيخ)\s+/, "").trim();
}

function buildQueryVariants(query) {
  const raw = String(query || "").trim();
  if (!raw) return [];

  const normalized = normalizeArabic(raw);
  const corrected = correctedWordsVariant(raw);
  const stripped = stripSheikhTitle(raw);

  const variants = new Set([raw, normalized, corrected, stripped]);

  for (const base of [raw, normalized, corrected, stripped]) {
    if (!base) continue;
    variants.add(base.replace(/(^|\s)ا/g, "$1أ"));
    variants.add(base.replace(/(^|\s)ا/g, "$1إ"));
    variants.add(base.replace(/(^|\s)ا/g, "$1آ"));
  }

  for (const group of KNOWN_ALIAS_GROUPS) {
    const normalizedGroup = group.map((item) => normalizeArabic(item));
    const groupMatched = normalizedGroup.some((item) => normalized.includes(item) || item.includes(normalized));
    if (groupMatched) {
      for (const item of group) variants.add(item);
    }
  }

  return [...variants].filter(Boolean);
}

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));
app.use((_req, res, next) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  next();
});

app.get("/", (_req, res) => {
  res.render("index", {
    query: "",
    results: [],
    error: null,
    searched: false,
  });
});

app.get("/search", async (req, res) => {
  const query = String(req.query.q || "").trim();
  const queryVariants = buildQueryVariants(query);

  if (!query) {
    return res.render("index", { query, results: [], error: null, searched: false });
  }

  try {
    const database = await getDb();
    await ensureLocalIndexes(database);

    const transcripts = database.collection("transcripts");

    const atlasPipeline = [
      {
        $search: {
          index: "transcripts_text_ar",
          compound: {
            should: [
              {
                text: {
                  query,
                  path: "text",
                  fuzzy: { maxEdits: 1 },
                  matchCriteria: "any",
                  score: { boost: { value: 4 } },
                },
              },
              {
                text: {
                  query: queryVariants,
                  path: "text",
                  fuzzy: { maxEdits: 2 },
                  matchCriteria: "any",
                },
              },
            ],
            minimumShouldMatch: 1,
          },
        },
      },
      ...joinedProjection(),
    ];

    const localTextPipeline = [
      {
        $match: {
          $text: {
            $search: queryVariants.join(" "),
          },
        },
      },
      {
        $addFields: {
          score: { $meta: "textScore" },
        },
      },
      { $sort: { score: -1 } },
      ...joinedProjection(),
    ];

    const regexFallbackPipeline = [
      {
        $match: {
          $or: queryVariants.map((item) => ({
            text: {
              $regex: toArabicFlexibleRegex(item),
              $options: "i",
            },
          })),
        },
      },
      ...joinedProjection(),
    ];

    let results = [];

    if (searchMode === "atlas") {
      results = await transcripts.aggregate(atlasPipeline).toArray();
      if (results.length === 0) {
        results = await transcripts.aggregate(regexFallbackPipeline).toArray();
      }
    } else {
      results = await transcripts.aggregate(localTextPipeline).toArray();
      if (results.length === 0) {
        results = await transcripts.aggregate(regexFallbackPipeline).toArray();
      }
    }

    const resultsWithContext = await enrichResultsWithContext(results, transcripts);

    return res.render("index", {
      query,
      results: resultsWithContext,
      error: null,
      searched: true,
    });
  } catch {
    return res.render("index", {
      query,
      results: [],
      searched: true,
      error:
        searchMode === "atlas"
          ? "search failed. check atlas search index and mongodb connection."
          : "search failed. check local mongodb container and imported data.",
    });
  }
});

export default app;

if (!process.env.VERCEL) {
  app.listen(port, () => {
    console.log(`sample app running on http://localhost:${port} (${searchMode} search)`);
  });
}