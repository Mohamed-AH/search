# App Blueprint (End-to-End)

Use this blueprint to recreate the current app exactly. Focus on **what** to build; reasoning is intentionally minimal.

## Stack & Runtime
- Node.js >= 20, ESM.
- Express + EJS.
- MongoDB (local + Atlas Search optional).
- Vanilla JS/CSS for client.
- Load `.env.local` via dotenv at startup.

## Environment Variables
- `MONGODB_URI` (default `mongodb://127.0.0.1:27017`)
- `MONGODB_DB` (default `audio_search_demo`)
- `SEARCH_MODE` = `local` | `atlas` (default `local`)
- `CONTEXT_WINDOW_SEC` (default `90`)
- `CONTEXT_ITEMS` (default `2`)
- `LOG_SEARCHES` (default `true`)
- `SEARCH_LOG_TTL_DAYS` (default `30`)

## Data Model
Collections:

**lectures**
- `shortId` (number)
- `titleArabic` (string)
- `audioUrl` (string)
- `audioFileName` (string)
- `sourceCsv` (string)
- `createdAt` / `updatedAt` (dates)

**transcripts**
- `lectureId` (ObjectId)
- `shortId` (number)
- `text` (string)
- `speaker` (string)
- `startTimeSec` (number)
- `startTimeMs` (number)
- `endTimeMs` (number)
- `sourceCsv` (string)

**search_logs** (no PII)
- `createdAt` (date)
- `query` (string)
- `normalizedQuery` (string)
- `tokens` (array)
- `minShouldMatch` (number)
- `resultCount` (number)
- `topLectureIds` (array of string ids, max 5)
- `searchMode` (string)
- `relevant` (null | true | false)
- `relevantAt` (date, optional)
- `comment` (string, max 300, optional)

## Indexes
- `transcripts`: text index `{ text: "text" }` for local search.
- `transcripts`: `{ lectureId: 1, startTimeSec: 1 }`, `{ shortId: 1 }`
- `lectures`: `{ shortId: 1 }`, `{ audioFileName: 1 }`
- `search_logs`: TTL index on `createdAt`, expire after `SEARCH_LOG_TTL_DAYS * 86400`.
- `search_logs`: `{ query: 1, createdAt: -1 }`

## Atlas Search Index
Name: `default` on `transcripts`.

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "text": {
        "type": "string",
        "analyzer": "lucene.arabic",
        "multi": {
          "keywordAnalyzer": {
            "type": "string",
            "analyzer": "lucene.keyword"
          }
        }
      }
    }
  }
}
```

## Server Behavior (Express)
**Initialization**
- Load `.env.local` with dotenv.
- Use EJS for views and serve `/public` statics.
- `express.urlencoded({ extended: false })` for feedback form.
- Set HTML content type with UTF‑8.

**GET /**
- Render `index.ejs` with:
  - `query: ""`
  - `results: []`
  - `error: null`
  - `searched: false`
  - `searchLogId: null`

**GET /search**
1. Build query variants:
   - Normalize Arabic (remove diacritics, normalize Hamza/Ya/Alif variants).
   - Word fixes: `اشيخ→الشيخ`, `النجمين→النجمي`, `يحياء→يحيى`.
   - Strip `الشيخ`/`شيخ` prefix.
   - Add Hamza variants.
   - Tokenize and include per‑word variants.
2. Token logic:
   - Stopwords: `بن`, `ابن`, `بنت`, `ال`, `و`, `في`, `على`, `من`, `عن`, `الى`, `إلى`, `مع`, `ثم`, `او`, `أو`, `هذا`, `هذه`, `ذلك`, `تلك`.
   - `minShouldMatch = min(2, tokenCount)`.
3. Search:
   - **Atlas mode** (`SEARCH_MODE=atlas`):
     - `$search` compound:
       - `must`: token `text` queries (path `text`, fuzzy 1) with `minimumShouldMatch`.
       - `should`:
         1) `phrase` on `text.keywordAnalyzer` (slop 2, boost 8)
         2) `text` on `text` (fuzzy 1, boost 4)
         3) `text` on variants (fuzzy 2)
   - **Local mode**: `$text` search on variants, fallback to regex.
4. Filter results by token hit count (>= minShouldMatch) using normalized text.
5. Enrich each result with context:
   - Fetch transcript lines for same lecture within ±`CONTEXT_WINDOW_SEC` of `startTimeSec`.
   - Exclude the hit itself.
   - Keep last `CONTEXT_ITEMS` before and first `CONTEXT_ITEMS` after.
6. Render `index.ejs` with results and `searched: true`.

**POST /search/feedback**
- Body: `logId`, `relevant` (`true`/`false`), optional `comment`.
- Validate `logId` ObjectId.
- Update `search_logs` doc: `relevant`, `relevantAt`, `comment` (max 300 chars).

**Search Logging**
- On each search: insert into `search_logs` with `relevant: null`.
- Fields: `createdAt`, `query`, `normalizedQuery`, `tokens`, `minShouldMatch`, `resultCount`, `topLectureIds` (max 5), `searchMode`.
- Best‑effort only; do not fail search on log error.

## UI (index.ejs)
- RTL Arabic layout (`dir="rtl"`), fonts: Noto Naskh Arabic + Scheherazade New.
- Warm gold/brown/cream palette, gradients, sticky header.
- Navigation links to `https://rasmihassan.com/...`.
- Search hero with Arabic title + subtitle.
- Search form: input + button “بحث”.
- Results render only after a search.
- Each result card:
  - Lecture title.
  - Speaker + timestamp line.
  - Inline context paragraph:
    - `contextBefore … [HIT in bold] … contextAfter`.
    - Strip timestamps from surrounding text.
  - “تشغيل من هنا” button.
- Feedback block **under search box**:
  - Only after search and only if `searchLogId` exists.
  - Toggle button opens feedback panel.
  - Two relevance buttons + optional comment + Send button.
  - After send, the feedback block hides until next search.

## Client JS (public/client.js)
- Audio player: clicking “تشغيل من هنا” loads audio and seeks to time.
- Feedback:
  - Toggle open/close.
  - Select relevance (active state).
  - Send button posts `/search/feedback` with `logId`, `relevant`, `comment`.
  - Hide feedback block after successful send.

## Import Scripts
**scripts/importLectureTranscripts.mjs**
- Matches transcript CSV files by shortId from filename (`225.csv` → shortId 225).
- Uses export file `data-export.txt` to map shortId to lecture metadata.
- Options: `--all`, `--file`, `--export`, `--dir`, `--dry-run`, `--replace`, `--resume`, `--reset-checkpoint`, `--checkpoint`, `--report`, `--min-lines`, `--stop-on-error`.
- Writes report + checkpoint JSON.

**scripts/generateMasterReport.mjs**
- Produces coverage report JSON+CSV from export file, DB, and checkpoint.
- Options: `--export`, `--checkpoint`, `--out`, `--outcsv`, `--min-lines`.

## Running
- Dev: `node server.mjs` (dotenv loads `.env.local`).
- App listens on `PORT` or `4000`.

## Vercel
- `server.mjs` exports Express app as default.
- `vercel.json` routes all requests to `api/index.mjs` (which imports server).