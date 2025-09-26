# AseanForge — ASEAN Tech Investment Report Generator

## Quickstart: One‑command pipeline

Prerequisites
- Python 3.12+ (recommended)
- Virtual env set up and dependencies installed
  - python -m venv venv && source venv/bin/activate
  - pip install -r requirements.txt
- .env file with: OPENAI_API_KEY, FIRECRAWL_API_KEY, NEON_DATABASE_URL
- Neon pgvector enabled: CREATE EXTENSION IF NOT EXISTS vector;

Run the full workflow from repo root:
```
bash scripts/run_full_pipeline.sh
```
What this does
- Initializes the database schema
- Ingests sources from config/sources.yaml with --limit-per-source 10 and writes embeddings to PGVector (inline)
- Generates a publish‑mode report for a sample topic (defaults to “Vietnam manufacturing FDI trends 2024”, k=10, model=o3-deep-research; falls back to o4-mini-deep-research if unsupported)
- Builds a PDF from the generated Markdown (ReportLab fallback if WeasyPrint libs missing)

Outputs
- Markdown + PDF in data/output/
- Progress timestamps and final artifact paths printed to the console

Troubleshooting
- If python isn’t found, create/activate a venv or run via ./venv/bin/python
- If WeasyPrint native libraries aren’t installed, the script falls back automatically to ReportLab (expected on macOS by default)
- Ensure .env is present and keys are valid; Neon URL must be reachable

Estimated runtime: ~5–10 minutes depending on network and source sites.

## Validation Test Run (example)

A recent end‑to‑end run (single command) produced the following:

Timestamps (UTC)
- 2025-09-23T07:07:47Z — Pipeline start
- 2025-09-23T07:07:47Z — Step 1/4 DB init
- 2025-09-23T07:07:49Z — Step 2/4 Ingest (config/sources.yaml, limit=10)
- 2025-09-23T07:13:15Z — Step 3/4 Report generation (publish, k=10, o3-deep-research  fallback to o4-mini-deep-research)
- 2025-09-23T07:13:47Z — Step 4/4 PDF build
- 2025-09-23T07:13:48Z — Pipeline complete

Artifacts
- Markdown: data/output/report_1758611596.md
- PDF:      data/output/report_1758611596.pdf

Executive Summary (snippet)
“In 2024, Vietnam continued to attract manufacturing FDI driven by robust regional integration, competitive labor costs and a strategic push into higher‑value sectors. Total inflows rose ~8%, led by electronics, automotive components and green technology. Key risks include rising wages in southern hubs and global supply‑chain disruptions.”


Local, simple pipeline:
Firecrawl (scrape) → Neon (pgvector via LangChain) → GPT‑4o (report) → WeasyPrint (PDF).

## 1) Setup
- Install Python 3.12+
- `python -m venv venv && source venv/bin/activate`
- `pip install -r requirements.txt`
- Copy `.env` and set `OPENAI_API_KEY`, `FIRECRAWL_API_KEY`, `NEON_DATABASE_URL`
- Optional: set other defaults in `.env`; report model is chosen by `--mode` unless overridden with `--model`
- Ensure pgvector is enabled on Neon: `CREATE EXTENSION IF NOT EXISTS vector;`

## 2) Ingest seed sources
```
python scripts/scrape_ingest.py --limit 5
```

- Dedup & cache behavior:
  - First run scrapes and stores a local cache at `data/ingest_cache.json` with content hash
  - Subsequent runs skip unchanged URLs and reuse cached content (pass `--refresh` to force re-scrape)
  - Very short/empty pages are skipped (tunable via `.env` `MIN_PAGE_CHARS`, default 300)

## 3) Generate a report (Markdown)
```
python scripts/generate_report.py --topic "AI investments in Vietnam" --timeframe "12-24 months"
```

- Model selection:
  - `--mode draft` (default) uses `o4-mini-deep-research` unless overridden with `--model`
  - `--mode publish` uses `o3-deep-research` unless unsupported, then falls back automatically to `o4-mini-deep-research`
  - You can always force a model with `--model` (aliases: `gpt-4o-mini`, `4o-mini`, `o4-mini`, `o4-mini-deep-research`, `o3-deep-research`)
  - The chosen mode and model are embedded in the report’s YAML front matter


## 3b) Report modes (draft vs publish)
Use modes to match your workflow stage:

- Draft mode (fast, minimal):
  ```
  python scripts/generate_report.py --mode draft --topic "AI investments in Vietnam" --timeframe "12-24 months"
  ```
  - Short prompt, minimal formatting (Executive Summary, Key Insights, Recommendations)
  - Skips the Sources & Notes appendix for speed
  - Designed for quick iteration and idea testing

- Publish mode (full, polished):
  ```
  python scripts/generate_report.py --mode publish --topic "AI investments in Vietnam" --timeframe "12-24 months"
  ```
  - Full structured prompt with all 7 sections and citation lines
  - Includes the full Sources & Notes appendix with reliability notes
  - Designed for final outputs and distribution


## 3a) Cost & token tracking (observability)
- At the end of each report generation run, the console prints a usage summary and a JSON line:
  - Example: `Report completed. Tokens used: 1234 input, 567 output. Estimated cost: $0.01`
  - JSON (one line) suitable for log aggregators: `{ "event": "usage_summary", ... }`
- Generated Markdown reports include YAML front matter fields:
  - `tokens_used` with `input` and `output` counts
  - `estimated_cost.total_usd`
  - `models_used` (array)
- Ingestion (embeddings) also logs a summary; embeddings token counts are estimated (~4 chars ≈ 1 token).
- Mode-aware styling:
  - build_pdf reads the `mode` from the report’s YAML front matter by default and applies:
    - draft: subtle DRAFT watermark + simple footer (date, brand)
    - publish: clean footer (date, brand, page numbers)
  - You can override explicitly with `--mode draft` or `--mode publish` if needed (default is `auto`).

## 3c) Metadata persistence (Neon Postgres)
- Initialize schema once:
  ```
  python scripts/init_db.py
  ```
- Set NEON_DATABASE_URL in .env (psycopg URL is auto-derived)
- What gets stored (minimal, normalized):
  - sources (domain/base_url) → pages (url, title, content_hash, fetched_at) → chunks (chunk_index, metadata)
  - runs table records each ingest/report run with tokens, cost, and for reports the chunk ids used
- Tracking is ON by default; disable with:
  - Ingest: `python scripts/scrape_ingest.py --no-track-metadata`
  - Reports: linkage to specific chunks is planned later; current version does not write report runs


- Pricing is defined in `scripts/usage_tracker.py` and can be updated if OpenAI pricing changes.

### Citations and Sources Appendix
- Each section ends with a standardized citation line:
  `[Citation: Title | domain | URL | accessed YYYY-MM-DD | snippets N]`
- A "Sources & Notes" appendix is appended automatically, listing:
  - Title and URL (with domain)
  - Accessed date
  - Number of snippets retrieved per source
  - A brief reliability note

Example citation line:
```
[Citation: Tech in Asia | www.techinasia.com | https://www.techinasia.com/ | accessed 2025-09-23 | snippets 2]
```


## 4) Build a PDF
```
python scripts/build_pdf.py --input data/output/report_XXXXXXXX.md --output data/output/report_XXXXXXXX.pdf
```

Notes:
- Add your logo to `assets/logo.png` (a placeholder is included)
- Tweak `assets/styles.css` for branding (colors, fonts)
- Extend `SEED_URLS` in `scripts/scrape_ingest.py` as needed


## Deep Research path (no‑fallback)

Prerequisites
- Access to OpenAI Deep Research models (`o3-deep-research`, `o4-mini-deep-research`)
- Python package `openai` installed (pip install openai)

Run via one‑command pipeline (strict, no fallbacks):
```
FORCE_DEEP_RESEARCH=1 bash scripts/run_full_pipeline.sh
```
Or directly:
```
python scripts/generate_report.py \
  --mode publish \
  --topic "ASEAN Tech Investment Intelligence — Q3 2025" \
  --timeframe "2024–2025" \
  --k 12 \
  --force-deep-research
```
Behavior
- Uses `o3-deep-research` in publish mode (draft uses `o4-mini-deep-research`)
- Fails immediately on access errors (no model fallback)
- Captures web sources used by Deep Research to:
  - `data/output/deep_research_sources_<ts>.json` (structured)
  - `data/output/deep_research_sources_<ts>.txt` (human‑readable)
- Adds a “Deep Research Sources” section to the report (separate from “Sources & Notes” appendix)

Feedback loop helper
```
python scripts/add_dr_sources_to_config.py
```
- Writes `config/sources_candidates_<ts>.yaml` with deduped domains/URLs and suggested limits
- Review and copy selected entries into `config/sources.yaml` manually
