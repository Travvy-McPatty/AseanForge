# ASEANForge Data Dictionary

## 1. Database Schema

- events
  - id (UUID, PK)
  - created_at (timestamp with time zone, default now())
  - authority (text)
  - title (text)
  - url (text)
  - summary_en (text, nullable)
  - embedding (vector(1536), nullable)
  - indexes: (authority), (created_at), (authority, created_at)

- documents
  - id (UUID, PK)
  - event_id (UUID, FK → events.id)
  - source_url (text)
  - content (text)
  - created_at (timestamp with time zone, default now())
  - indexes: (event_id), (created_at)
  - relation: many documents can reference one event (events.id)

- (optional) pages/chunks
  - If present, store intermediate page text or chunked text for embeddings

## 2. Field Meanings & Units

- events.created_at: UTC timestamp when the event was first ingested (ISO 8601)
- events.authority: ASEAN authority code (e.g., MAS, SC, IMDA)
- events.title: Extracted page title or headline
- events.url: Canonical URL of the event page (fall back when document URL is not available)
- events.summary_en: English summary (may be null)
- documents.content: Full extracted text content (HTML stripped, PDF parsed as text)
- documents.source_url: Canonical document URL (preferred when present)

## 3. Example Rows (illustrative)

```
Event:
 id: 123e4567-e89b-12d3-a456-426614174000
 created_at: 2025-09-29T12:34:56Z
 authority: MAS
 title: "MAS Issues Guidelines on Digital Payment Token Services"
 url: https://www.mas.gov.sg/regulation/guidelines/...
 summary_en: "The Monetary Authority of Singapore has issued new guidelines..."

Document:
 id: 223e4567-e89b-12d3-a456-426614174000
 event_id: 123e4567-e89b-12d3-a456-426614174000
 source_url: https://www.mas.gov.sg/-/media/...
 content: "This guideline sets out..."
```

## 4. Sampler & Alert Derivation Logic

- Samplers (7d, 24h):
  - Filter events by created_at >= NOW() - INTERVAL 'N hours/days'
  - LEFT JOIN documents on documents.event_id = events.id
  - Output CSV with pipe delimiter: ts|authority|title|url|preview_200
    - ts = ISO 8601 UTC (YYYY-MM-DDTHH:MM:SSZ)
    - url = documents.source_url if present else events.url
    - preview_200 = LEFT(documents.content, 200) else fallback to summary/title

- Alerts (keyword-based):
  - Load rules from configs/alerts.yaml
  - Scan events in a lookback window (default 168 hours)
  - Filter by rule authorities; case-insensitive OR-match across title, summary_en, content
  - Deduplicate by (rule_name, event_id); deterministic ordering for idempotency
  - Write deliverables/alerts_latest.csv and data/output/validation/latest/alerts_summary.txt

## 5. Diagram

```
┌─────────────┐       ┌──────────────┐
│   events    │──1:N──│  documents   │
│             │       │              │
│ id (PK)     │       │ id (PK)      │
│ authority   │       │ event_id (FK)│
│ title       │       │ source_url   │
│ created_at  │       │ content      │
└─────────────┘       └──────────────┘
       │
       ├──> samplers (filter by created_at, join, export CSV)
       └──> alerts (filter by created_at + authority, match keywords)
```

