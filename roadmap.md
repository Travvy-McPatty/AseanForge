## ASEANForge Roadmap (Lightweight)

A short, practical plan you can scan quickly and we can update as we go.

> Scope reminder: "ASEANForge is a local-first pipeline for ingesting sources, generating LLM research reports, and exporting branded PDFs"

### Core features working now
- Neon Postgres connection (SQLAlchemy + psycopg) verified
- Ingestion tested with Tech in Asia and DealStreetAsia; chunks written successfully
- Report generation to Markdown is working
- PDF output works via ReportLab (WeasyPrint is optional)
- Model workflow agreed:
  - 4o-mini → testing reports
  - o4-mini → draft/internal reports
  - o3 → published/sold reports
- Branding colors + logo are ready

### Near‑term improvements (small, focused)
- Runtime model switching
  - Default from .env; override via CLI flag (e.g., `--model`)
  - Persist chosen model in report metadata for traceability
- Source citations & metadata in reports
  - Include source name, URL, access date, and snippet count
  - Optional appendix: “Sources & Notes”
- Ingestion quality & safety
  - De-dup by URL + content hash; idempotent re-ingest
  - Simple caching for fetches to avoid re-scraping the same URL
  - Skip or flag empty/short pages
- Reliability & observability
  - Basic retries and structured error logs; summarize failures at end of run
  - Log token usage + estimated cost per run
- Simple, consistent CLI UX
  - Commands: `ingest`, `generate`, `export`; filters like `--since`, `--source`
- Report templates (done: draft/publish modes)
  - Phase 1: code-based templates per mode (completed); YAML/JSON externalization optional later
  - Track prompt/version in DB for reproducibility
- PDF polish (minimal, stable)
  - Cover page (logo, title, date); page numbers & footer
  - Standard font scheme (serif for body, sans-serif for headers)
  - Use brand colors tastefully; keep ReportLab as default
- Tests (local)
  - Unit tests for chunking, de-dup, and PDF generation
  - Smoke test for end-to-end generate→PDF
- Dependencies
  - Lock Python dependencies in requirements.txt for reproducibility
- Metadata persistence (completed)
  - Minimal normalized schema in Neon: sources → pages → chunks
  - Ingestion writes sources/pages/chunks; report YAML carries mode/model/topic/timeframe

### Later / optional (nice‑to‑have)
- WeasyPrint advanced styling
  - TOC, better typography, section headers with brand palette (#00205B / #BA0C2F / white)
- Automation scripts
  - Scheduled ingestion (cron/launchd) and batch report generation
- Light local UI (optional)
  - Select sources/date range, choose model, generate & download PDF/MD
- Distribution & sharing
  - Export bundles (PDF + MD + sources JSON); optional email via SMTP
- Retrieval & analysis
  - Simple vector search UI; cross-source analytics summaries
- Cost & usage tracking (advanced)
  - Aggregated usage dashboard; export CSV
- Security & multi-user (only if needed)
  - Local auth; roles for draft vs publish
- Packaging & deploy
  - Dockerfile for reproducible local runs; optional cloud deploy later
- Analytics-ready visuals
  - Matplotlib/Seaborn charts embedded into PDFs for key metrics
- Data quality & compliance
  - Basic PII scrubbing; broken-link detection

### Operating principles
- Stability over styling; ReportLab is default, WeasyPrint is opt‑in
- Local-first, simple setup; secrets via .env (python-dotenv)
- Keep config explicit and overridable via CLI flags
- Small, iterative changes; tests for core paths

### Quick checkpoints (to track as we go)
- [x] CLI `--model` with .env fallback and metadata capture
- [x] Log token usage + estimated cost per run
- [x] Source citation block in generated reports
- [x] Report templates (draft/publish modes)

- [x] Metadata persistence (sources/pages/chunks)

- [x] Draft/Publish PDF differentiation (watermark/footer)

- [x] De-dup + cache for ingestion; skip empty pages
- [x] Minimal cover page + footer in ReportLab PDFs
- [x] Unit + smoke tests passing locally
- [x] Lock dependencies in requirements.txt



### Milestone: One-Command Validation Run #2 (2025-09-23)
- Status: ✅ Completed
- Purpose: Confirm pipeline reproducibility with a different topic using the same single-command workflow.

Commands executed
- bash scripts/run_full_pipeline.sh
- TOPIC="ASEAN digital economy 2024" bash scripts/run_full_pipeline.sh

Timestamps (UTC)
- 2025-09-23T07:36:13Z — Pipeline start (Run #2)
- 2025-09-23T07:41:36Z — Step 3/4 Report generation (publish, k=10, o4-mini)
- 2025-09-23T07:42:08Z — Pipeline complete (Run #2)

Artifacts (Run #2)
- Markdown: data/output/report_1758613297.md
- PDF:      data/output/report_1758613297.pdf

Executive Summary snippet (Run #2)
“In 2024, ASEAN’s digital economy reached an estimated US $360 billion (12% of GDP), driven by e‑commerce, fintech and cross‑border data flows. Members advanced regulatory harmonization under the ASEAN Economic Community Blueprint 2025, while new partnership offices (e.g., ASEAN‑U.S.) and cooperation funds (AKCF) laid groundwork for expanded digital infrastructure and skills development.”

Validation checklist (Run #2)
- [x] Pipeline runs successfully with different topic
- [x] Both Markdown and PDF outputs generated
- [x] Executive Summary snippet displayed in console
- [x] Artifacts saved to data/output/
- [x] PDF uses AseanForge brand colors (ReportLab fallback with BRAND_BLUE #00205B and BRAND_RED #BA0C2F)



### Milestone: Internal Alpha Reports (2025-09-23)
- Status: ✅ Completed (2 publish-mode reports generated end-to-end)

Run #1 — Topic: EV supply chains in Thailand 2024 (timeframe: 2024)
- Timestamps (UTC)949664
  - Start: 2025-09-23T07:56:14Z
  - End:   2025-09-23T08:02:42Z
- Artifacts
  - Markdown: data/output/report_1758614531.md
  - PDF:      data/output/report_1758614531.pdf
  - Full log: data/output/logs/pipeline_20250923T075614Z_ev_thailand2024.log
- Executive Summary (snippet)
  “In 2024 Thailand emerged as Southeast Asia’s EV supply-chain hub, leveraging government incentives, local battery production, and strategic FDI. Key strengths include integrated battery assembly, growing CATL and Gulf Energy investments, and upstream nickel and cobalt refining. Challenges remain in raw-material sourcing, grid capacity, and skills gaps.”
- Validation
  - Citations include fresh access date: 2025-09-23 (confirmed in Sources & Notes)
  - Appendix present with snippet counts
  - YAML front matter includes mode: publish, model: o4-mini, tokens_used, estimated_cost

Run #2 — Topic: AI adoption in Vietnamese manufacturing 2024 (timeframe: 2024)
- Timestamps (UTC)
  - Start: 2025-09-23T08:02:53Z
  - End:   2025-09-23T08:07:15Z
- Artifacts
  - Markdown: data/output/report_1758614811.md
  - PDF:      data/output/report_1758614811.pdf
  - Full log: data/output/logs/pipeline_20250923T080253Z_ai_vn_mfg2024.log
- Executive Summary (snippet)
  “Vietnam’s manufacturing sector is entering a phase of AI-driven efficiency gains. Global AI investment is booming, and local players are piloting automation in high-volume production lines. Government incentives and rising labor costs are driving adoption in 2024.”
- Validation
  - Citations include fresh access date: 2025-09-23 (confirmed in Sources & Notes)
  - Appendix present with snippet counts
  - YAML front matter includes mode: publish, model: o4-mini, tokens_used, estimated_cost

Success Criteria (Internal Alpha Reports)
- [x] Minimum 2 complete reports (MD + PDF pairs) generated and archived this week
- [x] All citations show access dates within the current week (2025-09-23)
- [x] Executive Summaries are concise (2–3 sentences), investor-focused
- [x] Zero pipeline failures during production runs (4 steps completed successfully twice)
- [x] Reports saved to data/output/ with clear artifact paths
- [x] Content variety across ASEAN markets and tech sectors (Thailand EV supply chains; Vietnam AI in manufacturing)

### New milestone
- [ ] Minimal ingestion pipeline
  - [ ] Database schema migration for sources table
  - [ ] Configuration-driven source list (config/sources.yaml)
  - [ ] Core ingestion script using firecrawl-py
  - [ ] Metadata parsing and deduplication logic
  - [ ] Daily refresh workflow setup
  - [ ] Unit tests and smoke tests



## ASEANForge Roadmap (Milestone Journal)

> Scope: *ASEANForge is a local-first pipeline for ingesting sources, generating LLM research reports, and exporting branded PDFs.*
> Principle: *Focus on the simplest working MVP. Add polish only after reports are usable and repeatable.*

---

### ✅ Core Features (MVP foundation)
- Neon Postgres connection (SQLAlchemy + psycopg) verified
- Normalized schema in place: sources → pages → chunks
- Ingestion tested with config-driven source list; dedup + chunking working
- PGVector embeddings integrated inline during ingestion
- Report generation to Markdown working with citations + metadata
- PDF export working via ReportLab fallback with brand colors/logo
- Report templates:
  - **test** → gpt-4o-mini
  - **draft** → o4-mini-deep-research
  - **publish** → o3-deep-research
- Branding colors (#00205B / #BA0C2F / white) and logo applied
- One-command runner (`scripts/run_full_pipeline.sh`) executes init → ingest → report → PDF

---

### 📌 Milestones

#### Milestone: Minimal Ingestion Pipeline
- Status: ✅ Completed
- Deliverables:
  - Config-driven source list (`config/sources.yaml`)
  - Ingestion script (`scripts/ingest_sources.py`) with dedup + chunking
  - Dry-run mode + safe Neon writes
  - Unit test coverage for metadata extraction + chunking
- Outcome: Successfully wrote chunks into Neon from Reuters/Nikkei test run

---

#### Milestone: One-Command Validation Run #2 (2025-09-23)
- Status: ✅ Completed
- Purpose: Confirm reproducibility of the pipeline with a different topic using the single-command workflow

Artifacts (Run #2)
- Markdown: `data/output/report_1758613297.md`
- PDF:      `data/output/report_1758613297.pdf`

Executive Summary snippet (Run #2)
> “In 2024, ASEAN’s digital economy reached an estimated US $360 billion (12% of GDP), driven by e-commerce, fintech and cross-border data flows. Members advanced regulatory harmonization under the ASEAN Economic Community Blueprint 2025, while new partnership offices (e.g., ASEAN-U.S.) and cooperation funds (AKCF) laid groundwork for expanded digital infrastructure and skills development.”

Validation checklist
- [x] Pipeline runs successfully with different topic
- [x] Markdown + PDF outputs generated
- [x] Executive Summary snippet displayed in console
- [x] Artifacts saved to `data/output/`
- [x] PDF uses AseanForge brand colors (ReportLab fallback)

---

#### Milestone: Expanded Ingestion + 3 New Reports (2025-09-23)
- Status: ✅ Completed
- Purpose: Scale ingestion (limit=10 per source) and confirm report generation draws from fresh embeddings

Artifacts
- `data/output/report_1758610742.md/.pdf` — Vietnam manufacturing FDI trends 2024
- `data/output/report_1758610778.md/.pdf` — India–ASEAN supply chain integration 2024
- `data/output/report_1758610812.md/.pdf` — Taiwan semiconductor policy shifts 2024

Executive Summaries (snippets)
- **Vietnam FDI:** ≈ USD 25B inflows, led by electronics/EV batteries, CPTPP/RCEP integration; risks: land costs + skills gaps.
- **India–ASEAN:** Integration accelerated under RCEP/FTA; drivers: logistics, pharma/electronics diversification; challenges: infra + geopolitics.
- **Taiwan semiconductors:** Policy shifts to reinforce domestic capacity, streamline fab approvals, align export controls.

Checklist
- [x] Ingestion with limit=10 per source completed
- [x] Inline PGVector embeddings added
- [x] ingestion_summary.json produced
- [x] Three publish-mode reports generated (k=10, o4-mini-deep-research fallback used)
- [x] PDFs exported with brand styling
- [x] Executive Summaries captured

---

#### Milestone: Internal Alpha Reports (2025-09-23)
- Status: ✅ Completed (2 publish-mode reports generated end-to-end)
- Purpose: Validate full reports with citations, appendix, and metadata for internal review

**Run 1 — EV supply chains in Thailand 2024**
- Artifacts:
  - MD: `data/output/report_1758614531.md`
  - PDF: `data/output/report_1758614531.pdf`
  - Log: `data/output/logs/pipeline_20250923T075614Z_ev_thailand2024.log`
- Executive Summary:
  > Thailand emerged as Southeast Asia’s EV supply-chain hub in 2024, driven by incentives, FDI, and local battery assembly. Strengths include CATL/Gulf Energy projects and upstream refining, while challenges remain in sourcing, grid capacity, and skills gaps.

**Run 2 — AI adoption in Vietnamese manufacturing 2024**
- Artifacts:
  - MD: `data/output/report_1758614811.md`
  - PDF: `data/output/report_1758614811.pdf`
  - Log: `data/output/logs/pipeline_20250923T080253Z_ai_vn_mfg2024.log`
- Executive Summary:
  > Vietnam’s manufacturing sector in 2024 began piloting AI for automation and efficiency. Incentives and rising wages are pushing adoption, though execution risks remain.

Checklist
- [x] Two publish-mode reports generated via one-command pipeline
- [x] Fresh sources used (access date 2025-09-23 in appendix)
- [x] Citations + Sources & Notes appendix included
- [x] YAML front matter persisted (mode, model, tokens, cost)
- [x] Logs saved under `data/output/logs/`
- [x] PDFs branded via ReportLab fallback

---

### 🚧 Remaining MVP Steps
- Internal alpha circulation: share 5–10 reports with internal stakeholders for content review and calibration
- Weekly cadence: 1–2 topics per week, each logged with artifacts + Executive Summary in roadmap
- Light QA: check citation accuracy, appendix consistency, and formatting stability
- MVP completion = stable ingestion, retrieval, and PDF reports usable for **internal analysis + first demo sales**

---

### 📥 Backlog / Future Improvements (not MVP blockers)
- Reliability & observability (retries, error logs, token cost reporting)
- CLI polish (`ingest`, `generate`, `export` commands; filters like `--since`, `--source`)
- Advanced PDF styling (TOC, typography via WeasyPrint)
- Distribution bundles (PDF + MD + sources.json), optional email delivery
- Charts/visuals in reports (matplotlib/seaborn)
- Packaging/deploy (Dockerfile for reproducible local runs)

---

### Operating Principles
- MVP = ingestion → embeddings → report → PDF (stable + repeatable)
- Stability > styling (ReportLab default; WeasyPrint optional)
- Local-first, explicit config, secrets in `.env`
- Small, test-backed iterations



### Milestone: Internal Alpha Circulation (2025-09-23)
- Status: ✅ Completed (3 additional publish-mode reports for internal sharing)
- Purpose: Produce 2–3 more investor-grade topical reports and verify citations freshness and appendices.

Run A — Philippines fintech regulation 2024
- Timestamps (UTC)
  - Start: 2025-09-23T08:28:30Z
  - End:   2025-09-23T08:33:18Z
- Artifacts
  - Markdown: data/output/report_1758616369.md
  - PDF:      data/output/report_1758616369.pdf
  - Log:      data/output/logs/pipeline_20250923T082830Z_ph_finreg2024.log
- Executive Summary (snippet)
  “In 2024, the Bangko Sentral ng Pilipinas (BSP) continued to refine its digital-finance rulebook—issuing enhanced e-money-issuer guidelines, formalizing digital-bank licensing, and tightening AML/CFT standards. Mobile wallets and digital lenders are booming, but uneven compliance and infrastructure gaps pose risks.”
- Validation
  - Citations access date: 2025-09-23; Sources & Notes appendix present
  - YAML front matter includes mode: publish; model: o4-mini; tokens_used; estimated_cost

Run B — Indonesia EV battery supply chains 2024
- Timestamps (UTC)
  - Start: 2025-09-23T08:33:27Z
  - End:   2025-09-23T08:37:48Z
- Artifacts
  - Markdown: data/output/report_1758616642.md
  - PDF:      data/output/report_1758616642.pdf
  - Log:      data/output/logs/pipeline_20250923T083327Z_id_ev_battery2024.log
- Executive Summary (snippet)
  “Indonesia, home to over 50% of global nickel reserves, has positioned itself as a cornerstone of the EV battery supply chain. Government mandates and global partnerships drove rapid capacity build-up; challenges remain in high-grade refining and cell manufacturing.”
- Validation
  - Citations access date: 2025-09-23; Sources & Notes appendix present
  - YAML front matter includes mode: publish; model: o4-mini; tokens_used; estimated_cost

Run C — ASEAN cross-border digital payments 2024
- Timestamps (UTC)
  - Start: 2025-09-23T08:37:59Z
  - End:   2025-09-23T08:42:35Z
- Artifacts
  - Markdown: data/output/report_1758616923.md
  - PDF:      data/output/report_1758616923.pdf
  - Log:      data/output/logs/pipeline_20250923T083759Z_asean_xborder_payments2024.log
- Executive Summary (snippet)
  “ASEAN’s cross-border digital payments market reached an estimated US$25 billion in 2024, growing at ~15% YoY. Harmonized e-KYC and QR standards plus regional real-time rails are lowering costs and settlement times; interoperability gaps persist.”
- Validation
  - Citations access date: 2025-09-23; Sources & Notes appendix present
  - YAML front matter includes mode: publish; model: o4-mini; tokens_used; estimated_cost

Checklist (Internal Alpha Circulation)
- [x] 3 additional publish-mode reports generated (MD + PDF)
- [x] Logs saved to data/output/logs/ with clear filenames
- [x] Citations show current access date (2025-09-23) and appendix present
- [x] YAML front matter includes mode/model/tokens/cost
- [x] Zero pipeline failures across these runs; PDFs exported via ReportLab fallback


### Milestone: Client-Ready Sample Bundles (2025-09-24)
- Status: ✅ Completed
- Purpose: Produce two publish-mode reports with the new model tiers and package each into a single ZIP for easy sharing.

Artifacts
- **Topic A — ASEAN venture investment outlook 2024–2025**
  - MD: `data/output/report_1758673701.md`
  - PDF: `data/output/report_1758673701.pdf`
  - ZIP: `data/output/bundles/1758673701_bundle.zip`
  - Log: `data/output/logs/pipeline_20250924T001558Z_asean_venture_outlook.log`
  - Executive Summary (snippet): _“The venture capital landscape in Southeast Asia saw a notable downturn, with H1 2025 among the weakest periods in six years. Through 2024–2025, investors are shifting toward more sustainable, profitability-focused theses. Macro uncertainties are driving disciplined deployment and concentration in resilient sectors.”_

- **Topic B — Vietnam AI policy & compliance landscape 2024**
  - MD: `data/output/report_1758674411.md`
  - PDF: `data/output/report_1758674411.pdf`
  - ZIP: `data/output/bundles/1758674411_bundle.zip`
  - Log: `data/output/logs/pipeline_20250924T002906Z_vn_ai_policy.log`
  - Executive Summary (snippet): _“Vietnam is quickly advancing its AI ambitions with stepped-up regulation and institutional support in 2024. The landscape features clearer compliance expectations aligned to international standards, creating opportunities for well-governed entrants while raising the bar on data protection and operational controls.”_

Success criteria
- [x] New model mapping applied in code and docs (test=gpt-4o-mini, draft=o4-mini-deep-research, publish=o3-deep-research; fallback to o4-mini-deep-research if needed)
- [x] Two publish-mode reports generated (K=10) with citations and appendix
- [x] Each report has a ZIP bundle (PDF + MD + ingestion_summary.json + run log)
- [x] Fresh citation access dates reflect today
- [x] Artifacts and snippets recorded in roadmap.md


### Milestone: Internal Circulation Pack (2025-09-24)
- Status: ✅ Completed
- Purpose: Final MVP validation with three additional publish-mode reports for internal distribution.

Artifacts
- **Topic A — ASEAN green energy investment flows 2024**
  - MD: `data/output/report_1758675583.md`
  - PDF: `data/output/report_1758675583.pdf`
  - ZIP: `data/output/bundles/1758675583_bundle.zip`
  - Log: `data/output/logs/pipeline_20250924T004856Z_asean_green_energy_investment_flows_2024.log`
  - Executive Summary (snippet): _"In 2024, ASEAN is poised to experience significant growth in green energy investments, driven by increasing demand for sustainable energy solutions and supportive government policies."_

- **Topic B — Singapore digital banking landscape 2024**
  - MD: `data/output/report_1758676396.md`
  - PDF: `data/output/report_1758676396.pdf`
  - ZIP: `data/output/bundles/1758676396_bundle.zip`
  - Log: `data/output/logs/pipeline_20250924T010040Z_singapore_digital_banking_landscape_2024.log`
  - Executive Summary (snippet): _"Singapore's digital banking sector is poised for significant growth in 2024, driven by advancements in fintech, regulatory support, and increasing consumer demand for digital financial services. The Monetary Authority of Singapore (MAS) continues to foster a competitive environment, encouraging innovation while ensuring consumer protection."_

- **Topic C — Indonesia–Vietnam trade corridors 2024–2025**
  - MD: `data/output/report_1758677146.md`
  - PDF: `data/output/report_1758677146.pdf`
  - ZIP: `data/output/bundles/1758677146_bundle.zip`
  - Log: `data/output/logs/pipeline_20250924T011444Z_indonesia_vietnam_trade_corridors_2024_2025.log`
  - Executive Summary (snippet): _"The Indonesia-Vietnam trade corridor is poised for significant growth in 2024-2025, driven by enhanced digital infrastructure, government initiatives, and increasing bilateral trade."_

Success criteria
- [x] Three publish-mode reports generated with current model tier mapping
- [x] All reports include fresh citations (2025-09-24) and Sources & Notes appendix
- [x] ZIP bundles created containing MD + PDF + ingestion summary + run logs
- [x] MVP pipeline validation complete - ready for internal circulation


### Milestone: MVP Circulation Pack (2025-09-24)
- Status: ✅ Completed
- Purpose: Consolidate all MVP reports (7 total) into a single bundle for easy sharing with internal stakeholders.
- Artifacts: `data/output/bundles/all_reports_MVP_pack.zip`
- Checklist:
  - [x] All 7 reports included (MD + PDF + per-run logs + ingestion summary)
  - [x] Bundle saved under data/output/bundles/
  - [x] Roadmap updated with artifact path and status
  - [x] MVP pipeline validated and reproducible


### Milestone: Flagship Nearly-Saleable Report (2025-09-24)
- Status: ✅ Completed
- Topic: ASEAN Tech Investment Intelligence — Q3 2025
- Timeframe: 2024–2025
- Parameters: mode=publish, K=12, model target=o3-deep-research (fallbacks engaged to gpt-4o-mini)

Artifacts
- MD: `data/output/report_1758683669.md`
- PDF: `data/output/report_1758683669.pdf`
- ZIP: `data/output/bundles/1758683669_bundle.zip`
- Log: `data/output/logs/pipeline_20250924T031428Z_asean_tech_investment_intelligence_q3_2025.log`

Executive Summary (snippet)
> The first half of 2025 marked a significant downturn in venture capital fundraising across Southeast Asia, with dealmaking reaching its lowest levels in over six years. This trend reflects broader economic challenges and shifting investor sentiment, particularly in the tech sector.

Checklist
- [x] Targeted curated sources ingested (see `data/output/ingestion_summary.json`)
- [x] Enhanced retrieval depth applied (K=12)
- [x] Fresh citations show access date: 2025-09-24 (see Sources & Notes appendix)
- [x] MD + PDF + ZIP bundle saved under data/output/
- [x] Zero pipeline failures during execution (model fallbacks OK)



### Milestone: Deep Research Path Enabled (2025-09-24)
- Status: ✅ Completed
- Purpose: Add a strict no-fallback Deep Research path for publish-mode using OpenAI Responses API with o3-deep-research, and capture web sources used by the model.
- Features:
  - New `--force-deep-research` flag in `scripts/generate_report.py` (and `FORCE_DEEP_RESEARCH=1` support in `scripts/run_full_pipeline.sh`)
  - Direct OpenAI client path for `o3-deep-research` (publish) and `o4-mini-deep-research` (draft)
  - Strict error policy: fail fast on model access errors (no auto-fallback)
  - Deep Research sources saved to `data/output/deep_research_sources_<ts>.{json,txt}`
  - Report includes a “Deep Research Sources” section (separate from “Sources & Notes”)
  - Helper: `scripts/add_dr_sources_to_config.py` → writes `config/sources_candidates_<ts>.yaml` for manual curation
- Validation:
  - One-command pipeline run with `FORCE_DEEP_RESEARCH=1` completed with zero pipeline failures
  - Artifacts produced (MD + PDF + logs + deep_research_sources_*.json/txt)



### Milestone: Flagship Deep Research (LangChain, strict) — 2025-09-24
- Status: ✅ Completed (no fallbacks; LangChain DR path used)
- Parameters: mode=publish, K=12, backend=langchain, model=o3-deep-research (strict, fail-fast)
- Tokens/Cost (from usage log): 0 in / 0 out; total $0.00

Artifacts
- MD: `data/output/report_1758702197.md`
- PDF: `data/output/report_1758702197.pdf`
- ZIP: `data/output/bundles/1758702197_bundle.zip`
- DR Sources (JSON): `data/output/deep_research_sources_1758702197.json`
- DR Sources (TXT): `data/output/deep_research_sources_1758702197.txt`
- Usage log (JSONL): `data/output/logs/usage_1758702197.jsonl`
- Run log: `data/output/logs/pipeline_20250924T081937Z_asean_tech_investment_intelligence_q3_2025.log`

Executive Summary (snippet)
> ASEAN’s tech startup funding hit a six‑year low in H1 2025 (229 deals; ~$1.85B) as the funding winter persisted longer than expected. Investors turned highly selective, prioritising late‑stage deals and strong fundamentals; e‑commerce cooled while green tech gained momentum.

Validation
- [x] Strict DR mode enforced (no fallback); LangChain backend invoked
- [x] Report includes “Deep Research Sources” section + DR sources saved to JSON/TXT
- [x] YAML front matter includes mode/model/timestamp and usage fields
- [x] Markdown + PDF + ZIP bundle created with consistent timestamp suffixes


---

### Milestone: MVP Validation Steps (2025-09-24)
- Status: Partially ✅ (Steps 1 and 3 complete; Step 2 merged and queued for run)

Step 1 — Cost Tracking Validation (Responses backend)
- Run: `./venv/bin/python scripts/generate_report.py --topic "ASEAN fintech trends 2024" --timeframe "2024" --k 8 --mode draft --backend responses --force-deep-research`
- Artifacts:
  - MD: `data/output/report_1758706795.md`
  - PDF: `data/output/report_1758706795.pdf`
  - Usage log: `data/output/logs/usage_1758706795.jsonl`
- Result: Tokens recorded — 4,306 in / 1,070 out. Estimated cost currently $0.00 due to placeholder pricing in `.env` (very small per‑1M token rates). Action: update `PRICE_O4_MINI_DR_INPUT`/`PRICE_O4_MINI_DR_OUTPUT` to realistic values to show non‑zero USD.

Step 2 — Ingest Curated Sources
- Action completed: Merged candidates from `config/sources_candidates_1758702197.yaml` into `config/sources.yaml` under the correct sections (News & Analysis, Specialist, Official Data).
- Example new entries:
  - DealStreetAsia — SEA Startup Funding Report H1 2025
  - DealStreetAsia — SE Asia VC Funds Review H1 2025
  - TechEDT — SEA startup funding sinks to six-year low
  - DealStreetAsia Medium — Data Vantage (Zenith Learning FY24, etc.)
  - PolicyEdge — ASEAN Framework Agreement on Competition (AFAC)
  - ASEAN Secretariat (News)
- Next (requires permission due to external API/DB):
  - Dry run (no DB writes): `SOURCE_FILTER="techedt,dealstreetasia.medium,policyedge,asean.org/news" ./venv/bin/python scripts/ingest_sources.py --dry-run --limit-per-source 1`
  - Full ingest: `./venv/bin/python scripts/ingest_sources.py --limit-per-source 3`
  - Verify: `data/output/ingestion_summary.json` and Neon table counts.

Step 3 — Report Polish (Charts + Table)
- Implemented two brand‑styled charts and one structured table injected into Markdown and rendered in PDF (ReportLab):
  - Visuals: `data/output/visuals/trend_1758706795.png`, `data/output/visuals/distribution_1758706795.png`
  - Table: "Top Sectors" (Markdown table → ReportLab Table)
- Artifact: PDF above reflects visuals and table.

Summary
- ✅ Step 1: Token accounting validated; pricing config needs adjustment to display non‑zero cost
- ➡️ Step 2: Sources merged; awaiting approval to call Firecrawl/Neon to complete ingestion validation
- ✅ Step 3: Visual polish landed (2 charts + table), wired through to PDF



### Milestone: Cost Validation + Source Expansion + Investor-Grade Enhancements (2025-09-24)
- Status: ✅ Completed

Artifacts
- Responses DR cost validation (draft):
  - MD: `data/output/report_1758710577.md`
  - Usage log (JSONL): `data/output/logs/usage_1758710577.jsonl`
- Ingestion runs:
  - Dry-run summary: `data/output/ingestion_summary.json` (dry_run=True; previewed quality OK)
  - Full ingest summary: `data/output/ingestion_summary.json` (pages_added=7; chunks_added=57)
- Flagship report (LangChain, strict DR):
  - MD: `data/output/report_1758711870.md`
  - PDF: `data/output/report_1758711870.pdf`
  - ZIP: `data/output/bundles/1758711870_bundle.zip`

Token usage and cost (Responses validation run)
- Model: o4-mini-deep-research
- Tokens: 3,703 input / 919 output
- Estimated cost (USD): $0.007379 total

Executive Summary (Flagship, 2–3 sentences)
- Southeast Asia’s tech investment landscape cooled sharply through 2024–2025, hitting a six-year low by H1 2025 as investors turned more selective. Q2 2025 showed early signs of stabilization with funding more than doubling from Q1, led by resilient categories and quality late-stage names. Singapore still captured the majority of capital, while Vietnam and the Philippines saw relative strength in deal momentum.

Tables and Charts
- Charts (PNG files saved by pipeline):
  - `data/output/visuals/trend_1758711870.png`
  - `data/output/visuals/distribution_1758711870.png`
- Tables (embedded within Markdown; see sections in the file below):
  - `data/output/report_1758711870.md` → Tables > "Top 10 Deals" and "Sector Mix"

Validation checklist
- [x] Step 1 Cost tracking: Non-zero tokens and non-zero USD in JSONL usage log (Responses backend)
- [x] Step 2 Source expansion: Dry-run preview OK; full ingest wrote pages and chunks; summary saved
- [x] Step 3 Investor-grade polish: Exactly two charts and two compact tables added, plus a Methodology & Coverage section
- [x] Step 4 Flagship regenerated via LangChain strict DR; bundle created (MD + PDF + ZIP)
