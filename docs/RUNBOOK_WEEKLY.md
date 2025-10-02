# Weekly Maintenance Runbook

## Objective
Refresh AseanForge dataset with new regulatory announcements (last 7–90 days).

## Budget Caps (per run)
- Firecrawl: ≤600 URLs
- OpenAI Batch API: ≤$8
- Total runtime: ~20 minutes

## Steps

### 1. Harvest (≈10 min)
- Sitemap-first discovery for last 7–90 days
- Priority: MAS, BI, OJK, PDPC, SC, IMDA, SBV, MIC, DICT, BOT, BSP
- Link-backfill first (zero Firecrawl cost)
- Scrape only missing URLs (cap: 600)
- Log: data/output/weekly/{date}/harvest.log

### 2. Enrich (≈5 min)
- Batch API for new/updated events only
- Embeddings: text-embedding-3-small; Summaries: gpt-4o-mini-search-preview
- Budget: ≤$8
- Log: data/output/weekly/{date}/enrich.log

### 3. QA & Snapshot (≈5 min)
- Run coverage_expansion_step4_qa_kpis.py
- Run coverage_expansion_step5_sales_pack.py
- Archive: deliverables/weekly_snapshot_{date}.zip

### 4. Changelog
- Update CHANGELOG_WEEKLY.md with: date, events added, docs created, Firecrawl URLs used, Batch $ spent, coverage %, blockers

## Command
```bash
bash scripts/weekly_entrypoint.sh
```

## Rollback
If QA fails, restore from previous week's snapshot.

