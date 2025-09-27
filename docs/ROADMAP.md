## ASEANForge 60-day Roadmap

- [ ] Tier-1 ingestion pipeline (15 authorities) → target ≥9/15 inserting reliably
- [ ] Backfill window strategy and cadence decisions (weekly rolling windows; monthly deep dives)
- [ ] Daily Intel reports, Flash alerts, Monthly Deep Dive outputs (pipeline + templates)
- [ ] SLAs and metrics framework (latency SLOs, coverage %, quality thresholds, error budgets)
- [x] Database schema with pgvector embeddings (completed)
- [x] Core ingestion pipeline with idempotency (completed)
- [x] Initial Tier-1 seed URLs configuration (completed)

### Milestone themes
1. Reliability and Coverage
   - Harden network/egress constraints for .gov/.org (WAF, DNS)
   - Link discovery tuning within current limits (discover_links=8, process_per_source=5)
   - Alerting for authority failures (403/404/DNS/redirect)
2. Backfill Strategy
   - Two-pass approach: recent (60 days), deeper (12 months) per working authority
   - Append-only, idempotent inserts; verify with rerun items_new=0
3. Reporting
   - Daily/weekly PDFs with brand-styled charts; embed sources metadata
   - Executive dashboards: coverage by authority and trend lines
4. Quality & Evaluation
   - Sampling-based QA on summaries and classifications
   - Precision/recall signals for policy_area/action_type

### 30-day targets
- ≥9/15 authorities inserting reliably in live runs
- Tier-1 backfill for MAS, IMDA, PDPC, OJK, SC, BI, MIC
- CI smoke tests for dry-run/run and DB verification

### 60-day targets
- Attempt unlock of 2–4 blocked authorities via seed tuning (GET 200 only)
- Establish SLA/error budget and coverage KPIs; nightly backfill cadence
- Publish end-to-end validation bundle (PDF, MD, ingestion_summary.json, run log)

