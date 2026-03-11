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
const logSearches = String(process.env.LOG_SEARCHES || "true").toLowerCase() !== "false";
const searchLogTtlDays = Number(process.env.SEARCH_LOG_TTL_DAYS || 30);

let db;
let indexesReady = false;
let searchLogIndexesReady = false;
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

async function ensureSearchLogIndexes(database) {
  if (searchLogIndexesReady || !logSearches) return;
  const collections = await database.listCollections({ name: "search_logs" }).toArray();
  if (collections.length === 0) {
    await database.createCollection("search_logs");
  }
  const logs = database.collection("search_logs");
  await logs.createIndex({ createdAt: 1 }, { expireAfterSeconds: Math.max(1, Math.floor(searchLogTtlDays * 86400)) });
  await logs.createIndex({ query: 1, createdAt: -1 });
  searchLogIndexesReady = true;
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

const ARABIC_STOPWORDS = new Set([
  "بن",
  "ابن",
  "بنت",
  "ال",
  "و",
  "في",
  "على",
  "من",
  "عن",
  "الى",
  "إلى",
  "مع",
  "ثم",
  "او",
  "أو",
  "هذا",
  "هذه",
  "ذلك",
  "تلك",
]);

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

  const tokenized = [
    ...new Set(
      [raw, normalized, corrected, stripped]
        .flatMap((value) => String(value || "").split(/\s+/))
        .map((word) => word.trim())
        .filter(Boolean)
    ),
  ];

  for (const token of tokenized) {
    variants.add(token);
    variants.add(normalizeArabic(token));
  }

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

function buildTokenQuery(query) {
  const normalized = normalizeArabic(query);
  const tokens = normalized.split(" ").filter(Boolean);
  const filtered = tokens.filter((token) => !ARABIC_STOPWORDS.has(token));
  const effectiveTokens = filtered.length ? filtered : tokens;
  const minShouldMatch = Math.min(2, effectiveTokens.length || 1);

  return {
    normalized,
    tokens: effectiveTokens,
    minShouldMatch,
  };
}

function filterByTokenMatch(results, tokenInfo) {
  if (!Array.isArray(results) || results.length === 0) return results;
  const { tokens, minShouldMatch } = tokenInfo;
  if (!tokens || tokens.length <= 1) return results;

  return results.filter((result) => {
    const normalized = normalizeArabic(result.text || "");
    const hits = tokens.filter((token) => normalized.includes(token)).length;
    return hits >= minShouldMatch;
  });
}

async function logSearch(database, payload) {
  if (!logSearches) return;
  try {
    await ensureSearchLogIndexes(database);
    await database.collection("search_logs").insertOne(payload);
  } catch {
    // best-effort logging only
  }
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
  const tokenInfo = buildTokenQuery(query);

  if (!query) {
    return res.render("index", { query, results: [], error: null, searched: false });
  }

  try {
    const database = await getDb();
    await ensureLocalIndexes(database);

    const transcripts = database.collection("transcripts");

    const atlasTokenShould = tokenInfo.tokens.map((token) => ({
      text: {
        query: token,
        path: "text",
        fuzzy: { maxEdits: 1 },
        matchCriteria: "any",
      },
    }));

    const atlasPipeline = [
      {
        $search: {
          index: "default",
          compound: {
            must: [
              {
                compound: {
                  should: atlasTokenShould,
                  minimumShouldMatch: tokenInfo.minShouldMatch,
                },
              },
            ],
            should: [
              {
                phrase: {
                  query: tokenInfo.normalized,
                  path: "text.keywordAnalyzer",
                  slop: 2,
                  score: { boost: { value: 8 } },
                },
              },
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

    const filteredResults = filterByTokenMatch(results, tokenInfo);
    const resultsWithContext = await enrichResultsWithContext(filteredResults, transcripts);

    const topLectureIds = resultsWithContext
      .map((item) => item.lectureId)
      .filter(Boolean)
      .map((id) => String(id))
      .slice(0, 5);

    logSearch(database, {
      createdAt: new Date(),
      query,
      normalizedQuery: tokenInfo.normalized,
      tokens: tokenInfo.tokens,
      minShouldMatch: tokenInfo.minShouldMatch,
      resultCount: resultsWithContext.length,
      topLectureIds,
      searchMode,
      relevant: null,
    });

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