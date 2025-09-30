## ASEANForge Tier‑1 Ingestion SPEC

## Open Issues

### Pending Vendor Support (BNM & KOMINFO)
- BNM (Malaysia): 403 Forbidden errors despite stealth proxy, 15000ms waitFor, selectors, maxDepth 2
- KOMINFO (Indonesia): Zero content yield despite stealth proxy, 12000–15000ms waitFor, selectors, maxDepth 2
- Vendor packet: [firecrawl_vendor_packet.md](../data/output/validation/latest/firecrawl_vendor_packet.md)
- Status: Awaiting Firecrawl vendor response; unlock flags in `configs/vendor_overrides.yaml` set to `false`


### Scope
- Tier‑1 authority ingestion only (15 authorities)
- Firecrawl-first with HTTP fallback; global render=true with selective render=false overrides (PDPC, SC, OJK, BI)
- Storage: Neon Postgres (events, documents) with pgvector
- No OCR/cloud features beyond OpenAI + Firecrawl

### CLI
- `.venv/bin/python app/ingest.py [dry-run|run] --since=YYYY-MM-DD`
- Recommended env loading (no secret echo): `env $(grep -Ev '^#|^$' app/.env | xargs) .venv/bin/python app/ingest.py ...`
- Logs/artifacts for validations should write under `data/output/validation/latest/`

### Fetch Provider Policy (v2 SDK)
- Firecrawl v2 SDK is the primary provider (`scrape`/`crawl`) with exact docs parameters
  - scrape: formats=["markdown","html"], pageOptions={waitFor(ms), timeout, includeHtml}, parsers=["pdf"], proxy in {auto, stealth}
  - crawl: url, limit, pageOptions={waitFor(ms), timeout, includeHtml}, proxy, poll_interval=1, timeout=120
- Graceful degradation: v2 → legacy → HTTP fallback (HTTP only if FC returns empty)
- Authority overrides:
  - ASEAN, OJK → proxy=stealth, pageOptions.waitFor=5000
  - MAS, IMDA, BI → proxy=auto, pageOptions.waitFor=2000


#### Firecrawl v2-first configuration matrix and telemetry
- Primary API shapes (v2):
  - scrape: url=..., formats=["markdown","html"], pageOptions={waitFor: <ms>, timeout: 60000, includeHtml: true, parsePDF: true}, parsers=["pdf"], proxy in {auto, stealth}
  - crawl: url=..., limit=<n>, pageOptions={waitFor: <ms>, timeout: 60000, includeHtml: true, parsePDF: true}, proxy, poll_interval=1, timeout=120
- Graceful fallback to legacy signatures only on TypeError; record path in telemetry notes as v2|legacy
- Per-authority wait/proxy policy:
  - Stealth + 5000ms: ASEAN, OJK, BNM, MCMC, DICT
  - Auto + 2000ms: MAS, IMDA, BI, SC, PDPC, BOT, BSP, SBV, MIC, KOMINFO
- Telemetry CSVs (data/output/validation/latest/):
  - provider_events.csv: [authority, url, provider, status_code_or_error, waitFor_ms, proxy_mode, timestamp, notes]
  - provider_usage_sources.csv: [authority, url, fc_crawl_count, fc_scrape_count, http_fallback_count]
- Validation protocol:
  - Phase 1 (recent): scripts/ingest_sources.py --limit-per-source 3 --max-depth 1; rerun idempotency → items_new=0
  - Phase 2 (backfill): app/ingest.py run --since=2024-01-01; rerun idempotency → items_new=0


### Limits
- Max links discovered per source: 8
- Max links processed per source: 5
- Crawler: maxConcurrency=2; delayMs≈1200; maxDepth=1

### Idempotency
- event_hash = sha256(authority + normalized_url)
- Upserts: ON CONFLICT(event_hash) DO NOTHING on events/documents
- Rerun immediately after a successful run must produce items_new=0

### Redirect Policy
- http_get uses GET with benign User‑Agent
- Follows 301/302/307/308 up to 5 hops; aborts on >5 (loop protection)

### Security patterns
- App: `env $(grep -Ev '^#|^$' app/.env | xargs) .venv/bin/python ...`
- SQL: `env "$(grep '^NEON_DATABASE_URL=' app/.env)" psql "$NEON_DATABASE_URL" -c "..."`
- No secrets printed in logs

Seeds (authoritative: configs/firecrawl_seed.json; mirror in config/sources.yaml)
- ASEAN: https://asean.org/category/news/
- MAS: https://www.mas.gov.sg/news
- IMDA: https://www.imda.gov.sg/resources/press-releases-factsheets-and-speeches
- PDPC: https://www.pdpc.gov.sg/News-and-Events
- BI: https://www.bi.go.id/id/publikasi/ruang-media/news-release/
- OJK: https://www.ojk.go.id/id/berita-dan-kegiatan/siaran-pers
- KOMINFO: https://kominfo.go.id/siaran-pers
- BOT: https://www.bot.or.th/en/news-and-media/press-release
- BNM: https://www.bnm.gov.my/press-release
- SC: https://www.sc.com.my/resources/media/media-release
- MCMC: https://www.mcmc.gov.my/media/media-releases
- BSP: https://www.bsp.gov.ph/SitePages/Media%20and%20Research/MediaReleases.aspx
- DICT: https://dict.gov.ph/category/press-releases/
- MIC: https://english.mic.gov.vn/Pages/TinTuc/tintuckinhte.aspx
- SBV: https://www.sbv.gov.vn/webcenter/portal/en/home/sbv/news/press-release

Success criteria
- 	≥9/15 authorities have items_new>0 in run.log since --since date
- 	Idempotency: rerun items_new=0 and DB counts unchanged


Seeds table (authoritative; mirrored in config/sources.yaml)

Authority | URL | render_required | selectors
---|---|---|---
ASEAN | https://asean.org/category/news/ | true | -
MAS | https://www.mas.gov.sg/news | true | article .article-content, .article-content
IMDA | https://www.imda.gov.sg/resources/press-releases-factsheets-and-speeches | true | .rich-text, .content
PDPC | https://www.pdpc.gov.sg/News-and-Events | false | -
BI | https://www.bi.go.id/id/publikasi/ruang-media/news-release/ | false | -
OJK | https://www.ojk.go.id/id/berita-dan-kegiatan/siaran-pers | false | -
KOMINFO | https://kominfo.go.id/siaran-pers | true | -
BOT | https://www.bot.or.th/en/news-and-media/press-release | true | -
BNM | https://www.bnm.gov.my/press-release | true | -
SC | https://www.sc.com.my/resources/media/media-release | false | -
MCMC | https://www.mcmc.gov.my/media/media-releases | true | -
BSP | https://www.bsp.gov.ph/SitePages/Media%20and%20Research/MediaReleases.aspx | true | -
DICT | https://dict.gov.ph/category/press-releases/ | true | -
MIC | https://english.mic.gov.vn/Pages/TinTuc/tintuckinhte.aspx | true | -
SBV | https://www.sbv.gov.vn/webcenter/portal/en/home/sbv/news/press-release | true | -
