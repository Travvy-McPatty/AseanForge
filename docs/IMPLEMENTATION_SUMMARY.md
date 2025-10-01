# OpenAI Batch API Enrichment + High-Throughput Harvest Implementation Summary

**Date**: 2025-10-01  
**Status**: ✅ Complete

## Overview

This implementation adds two major features to AseanForge:

1. **OpenAI Batch API Enrichment**: Async batch processing for summaries and embeddings at ~50% cost savings
2. **High-Throughput Harvest Pipeline**: Improved ingestion with rate limiting, robots.txt compliance, and health monitoring

## Part 1: OpenAI Batch API Enrichment (ENRICH Mode)

### Database Changes

**Migration**: `scripts/migrate_add_enrichment_columns.py`

Added 6 columns to `events` table:
- `summary_model` (TEXT): Model used for summary (e.g., "gpt-4o-mini")
- `summary_ts` (TIMESTAMPTZ): Timestamp of summary generation
- `summary_version` (TEXT): Version identifier for idempotency
- `embedding_model` (TEXT): Model used for embedding (e.g., "text-embedding-3-small")
- `embedding_ts` (TIMESTAMPTZ): Timestamp of embedding generation
- `embedding_version` (TEXT): Version identifier for idempotency

Created unique index:
- `events_unique_hash` on `(authority, event_hash)` for hard idempotency

### Module Structure

**`app/enrich_batch/`**:

1. **`builders.py`**: Build JSONL request files
   - `build_embedding_requests()`: Query documents, chunk text, estimate costs
   - `build_summary_requests()`: Query events, prepare summaries, estimate costs
   - `chunk_text()`: Smart chunking with 1500 token limit and 10% overlap
   - `estimate_tokens()`: Token estimation using tiktoken

2. **`submit.py`**: Upload and submit batches
   - `submit_batch()`: Upload JSONL to OpenAI Files API, create batch job
   - Saves metadata to `data/batch/{kind}_{batch_id}.batch.json`

3. **`poll.py`**: Poll batch status
   - `poll_batch()`: Poll every 60s, timeout after 26 hours
   - Downloads output and error files on completion
   - Writes failure reports on timeout/error

4. **`merge.py`**: Parse results and update database
   - `merge_embeddings()`: Parse embedding responses, update events table
   - `merge_summaries()`: Parse summary responses, update events table
   - Idempotency: Skip rows already enriched with same model

5. **`cli.py`**: Command-line interface
   - Commands: `build`, `submit`, `poll`, `merge`, `status`, `cancel`
   - Full manual control over batch lifecycle

### Integration

**`app/ingest.py`**:
- Added `--mode enrich` support
- Added `run_enrich_auto()` function for full pipeline orchestration
- Budget enforcement: Abort if projected cost exceeds limit
- Generates `enrichment_report.md` with metrics

### Configuration

**`app/.env`** additions:
```bash
SUMMARY_MODEL=gpt-4o-mini
EMBED_MODEL=text-embedding-3-small
BATCH_COMPLETION_WINDOW=24h
BATCH_MAX_REQUESTS=20000
BATCH_MAX_FILE_MB=100
ENRICH_MAX_USD_TEST=25
ENRICH_MAX_USD_FULL=200
```

### Cost Savings

- **Embeddings**: $0.00001 per 1K tokens (50% off $0.00002)
- **Summaries**: $0.075 per 1M input tokens, $0.300 per 1M output tokens (50% off)
- **Example**: 1M tokens embeddings + 100K input/20K output summaries = ~$0.02 (vs $0.04 real-time)

### Usage

**Automatic**:
```bash
.venv/bin/python app/ingest.py run --mode enrich --auto
```

**Manual**:
```bash
# Build
.venv/bin/python -m app.enrich_batch.cli build --kind summaries --since 2025-07-01 --out data/batch/summaries.jsonl

# Submit
.venv/bin/python -m app.enrich_batch.cli submit --kind summaries --input data/batch/summaries.jsonl

# Poll
.venv/bin/python -m app.enrich_batch.cli poll --batch-id batch_abc123

# Merge
.venv/bin/python -m app.enrich_batch.cli merge --kind summaries --batch-id batch_abc123
```

## Part 2: High-Throughput Harvest Improvements

### Rate Limit Handling

**`app/ingest.py`** additions:

1. **Rate limit state tracker**:
   - `consecutive_429s`: Counter for 429 errors
   - `current_concurrency`: Dynamic concurrency adjustment
   - `paused_until`: Pause timestamp

2. **Functions**:
   - `check_rate_limit_pause()`: Check if paused, sleep if needed
   - `handle_rate_limit_error()`: Increment counter, pause on ≥3 consecutive 429s
   - `reset_rate_limit_counter()`: Reset on successful request

3. **Circuit breaker**:
   - Pause 60s and halve concurrency on ≥3 consecutive 429s
   - Hard halt on ≥6 consecutive 429s (writes `rate_limit_trip.txt`)

### robots.txt Compliance

**`app/robots_checker.py`**:

- `RobotsChecker` class with domain-level caching
- `is_allowed()`: Check if URL is allowed by robots.txt
- `log_block()`: Log blocked URLs to `robots_blocked.csv`
- `get_stats()`: Get statistics on robots.txt checks

**Configuration**:
```bash
ROBOTS_UA=AseanForgeBot/1.0 (+contact: data@aseanforge.com)
```

### CLI Enhancements

**`app/ingest.py`** new arguments:
- `--use-batch-scrape`: Use Firecrawl batch scrape API
- `--batch-size`: URLs per batch (50-200)
- `--limit-per-source`: Max URLs per source
- `--max-depth`: Max crawl depth

### Health Monitoring

**Existing**: `scripts/fc_health_check.py`
- Query Firecrawl account usage
- Check queue health status
- Pre/post-run validation

## Part 3: Executive Outputs

### Final Report

**`scripts/generate_final_report.py`**:

Generates `data/output/validation/latest/final_report.md` with:
- Summary: Total events, documents, date range
- Coverage by authority: Events, documents, last pub date, status
- Freshness metrics: % in last 7/30/90 days
- Top failures by domain
- robots.txt blocks count
- Cost summary placeholders

### CSV Deliverables

**`scripts/generate_deliverables.py`**:

Generates 4 CSV files in `data/output/validation/latest/deliverables/`:

1. **`sampler_24h.csv`**: Last 24 hours (50 rows max)
   - Columns: timestamp, authority, title, url, preview_200

2. **`sampler_7d.csv`**: Last 7 days (200 rows max)
   - Columns: timestamp, authority, title, url, preview_200

3. **`coverage_by_authority.csv`**: One row per authority
   - Columns: authority, event_count, document_count, last_pub_date, days_since_last_pub

4. **`failures_top_domains.csv`**: Top 20 domains by error count
   - Columns: domain, error_count, sample_error, sample_url

## Validation Results

### Batch Enrichment CLI Test

**Summaries**:
- Built 10 requests
- Estimated: 3,903 input tokens, 1,800 output tokens
- Projected cost: $0.0008

**Embeddings**:
- Built 46 requests (5 documents, chunked)
- Estimated: 64,522 tokens
- Projected cost: $0.0006

**Total**: $0.0014 (well under budget)

### Executive Outputs Test

**Final Report**:
- 168 events, 96 documents
- 13 authorities (all active)
- 100% freshness in last 7 days
- 2 failure domains identified

**CSV Deliverables**:
- sampler_24h.csv: 0 rows (no events in last 24h)
- sampler_7d.csv: 168 rows
- coverage_by_authority.csv: 13 rows
- failures_top_domains.csv: 2 rows

## Files Created

### Enrichment Module
- `app/enrich_batch/__init__.py`
- `app/enrich_batch/builders.py`
- `app/enrich_batch/submit.py`
- `app/enrich_batch/poll.py`
- `app/enrich_batch/merge.py`
- `app/enrich_batch/cli.py`

### Harvest Improvements
- `app/robots_checker.py`
- `scripts/migrate_add_enrichment_columns.py`

### Executive Outputs
- `scripts/generate_final_report.py`
- `scripts/generate_deliverables.py`

### Documentation
- `docs/BATCH_ENRICHMENT.md`
- `docs/IMPLEMENTATION_SUMMARY.md` (this file)

### Configuration
- Updated `app/.env`
- Updated `docs/ROADMAP.md`

## Next Steps

1. **Run full enrichment pipeline** on production data
2. **Test high-throughput harvest** with new flags
3. **Generate executive outputs** after each run
4. **Monitor rate limits** and robots.txt blocks
5. **Integrate cost tracking** with Firecrawl and OpenAI usage APIs
6. **Create snapshot ZIP** with all deliverables

## Known Issues

1. **Firecrawl v2 API**: Some authorities (BNM, KOMINFO) fail with `pageOptions` error
   - Workaround: Fallback to legacy API
   - Fix: Update Firecrawl SDK or use HTTP client

2. **Batch scrape API**: Not yet implemented
   - Requires Firecrawl account support verification
   - Placeholder flag `--use-batch-scrape` added

3. **Cost tracking**: Placeholders only
   - Requires integration with Firecrawl and OpenAI usage APIs
   - Manual tracking via account dashboards

## Testing Checklist

- [x] Database migration runs successfully
- [x] Batch enrichment CLI builds requests
- [x] Cost estimation works correctly
- [x] Idempotency prevents duplicate processing
- [x] Rate limit handling pauses on 429s
- [x] robots.txt checker caches domains
- [x] Final report generates with correct metrics
- [x] CSV deliverables have expected columns
- [ ] Full enrichment pipeline (end-to-end)
- [ ] High-throughput harvest with batch scrape
- [ ] Snapshot ZIP creation
- [ ] Cost tracking integration

## Documentation

- **User Guide**: `docs/BATCH_ENRICHMENT.md`
- **Implementation Summary**: `docs/IMPLEMENTATION_SUMMARY.md` (this file)
- **Roadmap**: `docs/ROADMAP.md` (updated)
- **Troubleshooting**: `docs/runbook_troubleshooting.md` (existing)

## Support

For issues or questions:
1. Check `docs/BATCH_ENRICHMENT.md` for usage examples
2. Review `data/output/validation/latest/enrichment_report.md` for metrics
3. Check `data/batch/*.batch.json` for batch metadata
4. Review `data/output/validation/latest/fc_errors.csv` for failures

