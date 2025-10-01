# OpenAI Batch API Enrichment Guide

## Overview

The Batch Enrichment module provides async batch processing for summaries and embeddings at ~50% cost savings compared to real-time API calls.

## Features

- **Cost Savings**: ~50% discount via OpenAI Batch API
- **Idempotency**: Skip rows already enriched with same model version
- **Budget Enforcement**: Abort if projected cost exceeds budget
- **Chunking**: Smart text chunking for embeddings (1500 tokens with 10% overlap)
- **Version Tracking**: Track model, timestamp, and version for all enrichments

## Quick Start

### 1. Automatic Pipeline (Recommended)

Run the full enrichment pipeline automatically:

```bash
.venv/bin/python app/ingest.py run --mode enrich --auto
```

This will:
1. Build embedding and summary requests
2. Check projected cost against budget
3. Submit both batches to OpenAI
4. Poll until completion (up to 26 hours)
5. Merge results to database
6. Generate enrichment report

### 2. Manual Control

For more control, use the CLI directly:

#### Build Requests

```bash
# Build embedding requests
.venv/bin/python -m app.enrich_batch.cli build \
  --kind embeddings \
  --since 2025-07-01 \
  --limit 20000 \
  --out data/batch/embeddings.requests.jsonl

# Build summary requests
.venv/bin/python -m app.enrich_batch.cli build \
  --kind summaries \
  --since 2025-07-01 \
  --limit 10000 \
  --out data/batch/summaries.requests.jsonl
```

#### Submit Batches

```bash
# Submit embeddings
.venv/bin/python -m app.enrich_batch.cli submit \
  --kind embeddings \
  --input data/batch/embeddings.requests.jsonl

# Submit summaries
.venv/bin/python -m app.enrich_batch.cli submit \
  --kind summaries \
  --input data/batch/summaries.requests.jsonl
```

#### Poll Status

```bash
.venv/bin/python -m app.enrich_batch.cli poll --batch-id batch_abc123
```

#### Merge Results

```bash
# Merge embeddings
.venv/bin/python -m app.enrich_batch.cli merge \
  --kind embeddings \
  --batch-id batch_abc123

# Merge summaries
.venv/bin/python -m app.enrich_batch.cli merge \
  --kind summaries \
  --batch-id batch_def456
```

## Configuration

Add to `app/.env`:

```bash
# Batch Enrichment Settings
SUMMARY_MODEL=gpt-4o-mini
EMBED_MODEL=text-embedding-3-small
BATCH_COMPLETION_WINDOW=24h
BATCH_MAX_REQUESTS=20000
BATCH_MAX_FILE_MB=100
ENRICH_MAX_USD_TEST=25
ENRICH_MAX_USD_FULL=200
```

## Database Schema

The migration adds these columns to the `events` table:

```sql
-- Summary tracking
summary_model TEXT
summary_ts TIMESTAMPTZ
summary_version TEXT

-- Embedding tracking
embedding_model TEXT
embedding_ts TIMESTAMPTZ
embedding_version TEXT
```

## Cost Estimation

### Embeddings (text-embedding-3-small)

- **Base cost**: $0.00002 per 1K tokens
- **Batch discount**: 50%
- **Effective cost**: $0.00001 per 1K tokens

Example: 1M tokens = $0.01

### Summaries (gpt-4o-mini)

- **Input cost**: $0.150 per 1M tokens
- **Output cost**: $0.600 per 1M tokens
- **Batch discount**: 50%
- **Effective input cost**: $0.075 per 1M tokens
- **Effective output cost**: $0.300 per 1M tokens

Example: 100K input + 20K output = $0.0135

## Idempotency

The system ensures idempotency by:

1. **Version checking**: Only update if `model_version` differs
2. **Unique index**: `(authority, event_hash)` prevents duplicates
3. **Conditional updates**: SQL `WHERE` clause checks current model

Example:
```sql
UPDATE events SET
  summary_en = %s,
  summary_model = %s,
  summary_ts = NOW(),
  summary_version = %s
WHERE event_id = %s
  AND (summary_model IS NULL OR summary_model != %s)
```

## Monitoring

### Check Batch Status

```bash
.venv/bin/python -m app.enrich_batch.cli status --batch-id batch_abc123
```

### View Enrichment Report

After completion, check:
```
data/output/validation/latest/enrichment_report.md
```

### Cancel Running Batch

```bash
.venv/bin/python -m app.enrich_batch.cli cancel --batch-id batch_abc123
```

## Troubleshooting

### Batch Timeout

If a batch times out after 26 hours:
1. Check `data/output/validation/latest/batch_<id>_timeout.txt`
2. Verify OpenAI Batch API status
3. Retry with smaller batch size

### Cost Exceeded Budget

If projected cost exceeds budget:
1. Reduce `--limit` parameter
2. Increase `ENRICH_MAX_USD_FULL` in `.env`
3. Run in smaller batches

### Merge Errors

If merge fails:
1. Check `data/batch/<batch_id>.errors.jsonl` for failed requests
2. Verify database connection
3. Check for schema changes

## Best Practices

1. **Test first**: Use `ENRICH_MAX_USD_TEST` for small test runs
2. **Monitor costs**: Review `enrichment_report.md` after each run
3. **Incremental enrichment**: Use `--since` to process recent data first
4. **Verify idempotency**: Rerun same batch to confirm `skipped_count` matches
5. **Check versions**: Ensure `summary_version` and `embedding_version` are consistent

## Example Workflow

```bash
# 1. Run migration (one-time)
.venv/bin/python scripts/migrate_add_enrichment_columns.py

# 2. Test with small batch
.venv/bin/python -m app.enrich_batch.cli build \
  --kind summaries \
  --since 2025-09-01 \
  --limit 100 \
  --out data/batch/test.jsonl \
  --budget 1.0

# 3. Submit and poll
.venv/bin/python -m app.enrich_batch.cli submit \
  --kind summaries \
  --input data/batch/test.jsonl

.venv/bin/python -m app.enrich_batch.cli poll --batch-id <batch_id>

# 4. Merge results
.venv/bin/python -m app.enrich_batch.cli merge \
  --kind summaries \
  --batch-id <batch_id>

# 5. Verify in database
psql "$NEON_DATABASE_URL" -c "SELECT COUNT(*) FROM events WHERE summary_model = 'gpt-4o-mini';"

# 6. Run full pipeline
.venv/bin/python app/ingest.py run --mode enrich --auto
```

## API Reference

See module docstrings for detailed API documentation:
- `app/enrich_batch/builders.py`
- `app/enrich_batch/submit.py`
- `app/enrich_batch/poll.py`
- `app/enrich_batch/merge.py`
- `app/enrich_batch/cli.py`

