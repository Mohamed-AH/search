# Express + EJS Arabic Transcript Search

## local docker mongodb (for now)

1. start mongodb container:
   - `docker compose up -d`
2. set env vars:
   - `MONGODB_URI=mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin`
   - `MONGODB_DB=audio_search_demo`
   - `SEARCH_MODE=local`
3. import sample data:
   - `npm run sample:import`
4. run app:
   - `npm run sample:dev`
5. open:
   - `http://localhost:4000`

## search modes

- `SEARCH_MODE=local` uses mongodb text search (`$text`) and works with docker/local mongodb.
- `SEARCH_MODE=atlas` uses atlas `$search` with fuzzy matching (`maxEdits: 1`) and needs an atlas search index named `transcripts_text_ar` on `transcripts.text` with `lucene.arabic`.

## csv import

- default file: `sample-app/scripts/sample-transcripts.csv`
- custom file:
  - `node sample-app/scripts/importCsv.mjs ./your-file.csv`

required csv columns:
- `text`
- `startTimeSec`
- `speaker`
- `titleArabic`
- `audioUrl`
- `audioFileName`