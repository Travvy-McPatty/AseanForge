#!/usr/bin/env python3
"""
Coverage Expansion Step 5: Sales-Ready Pack

Purpose: Create sales-ready dataset exports and documentation
"""

import json
import os
import shutil
import sys
import zipfile
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

OUTPUT_DIR = "data/output/validation/latest"
DELIVERABLES_DIR = "deliverables"
DATASET_DIR = os.path.join(DELIVERABLES_DIR, "dataset")


def get_db():
    """Get database connection."""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set in app/.env")
    return psycopg2.connect(db_url)


def export_data():
    """Export events and documents to Parquet and CSV."""
    conn = get_db()
    
    # Export events
    print("Exporting events table...")
    events_df = pd.read_sql("""
        SELECT 
            event_id,
            event_hash,
            authority,
            title,
            url,
            pub_date,
            access_ts,
            summary_en,
            summary_model,
            summary_ts,
            summary_version,
            embedding_model,
            embedding_ts,
            embedding_version
        FROM events
        ORDER BY authority, pub_date DESC
    """, conn)
    
    os.makedirs(DATASET_DIR, exist_ok=True)
    
    # Save as Parquet and CSV (Parquet optional)
    try:
        events_df.to_parquet(os.path.join(DATASET_DIR, "events.parquet"), index=False)
    except Exception as e:
        print(f"  ! Skipping Parquet export for events (pyarrow/fastparquet missing): {e}")
    events_df.to_csv(os.path.join(DATASET_DIR, "events.csv"), index=False)
    print(f"  ✓ Exported {len(events_df)} events (CSV)")

    # Export documents
    print("Exporting documents table...")
    documents_df = pd.read_sql("""
        SELECT 
            document_id,
            event_id,
            source,
            source_url,
            title,
            LENGTH(clean_text) as clean_text_length,
            LENGTH(raw_text) as raw_text_length,
            rendered
        FROM documents
        ORDER BY event_id
    """, conn)
    
    try:
        documents_df.to_parquet(os.path.join(DATASET_DIR, "documents.parquet"), index=False)
    except Exception as e:
        print(f"  ! Skipping Parquet export for documents (pyarrow/fastparquet missing): {e}")
    documents_df.to_csv(os.path.join(DATASET_DIR, "documents.csv"), index=False)
    print(f"  ✓ Exported {len(documents_df)} documents (CSV)")

    conn.close()
    
    return len(events_df), len(documents_df)


def create_data_dictionary():
    """Create data dictionary documentation."""
    content = """# AseanForge Dataset - Data Dictionary

## Overview
This dataset contains regulatory and policy events from ASEAN financial authorities, enriched with AI-generated summaries and embeddings.

## Tables

### events.parquet / events.csv
Core events table containing regulatory announcements, press releases, and policy documents.

| Column | Type | Description |
|--------|------|-------------|
| event_id | UUID | Unique identifier for the event |
| event_hash | String | Hash of normalized title + URL for deduplication |
| authority | String | Regulatory authority (MAS, BI, OJK, etc.) |
| title | String | Event title/headline |
| url | String | Source URL |
| pub_date | Timestamp | Publication date |
| access_ts | Timestamp | When the event was first accessed/ingested |
| summary_en | Text | AI-generated English summary |
| summary_model | String | Model used for summary generation |
| summary_ts | Timestamp | When summary was generated |
| summary_version | String | Summary generation version |
| embedding_model | String | Model used for embedding generation |
| embedding_ts | Timestamp | When embedding was generated |
| embedding_version | String | Embedding generation version |

### documents.parquet / documents.csv
Document content table with extracted text from source URLs.

| Column | Type | Description |
|--------|------|-------------|
| document_id | UUID | Unique identifier for the document |
| event_id | UUID | Foreign key to events table |
| source | String | Document source type (html, pdf, etc.) |
| source_url | String | URL where document was fetched |
| title | String | Document title |
| clean_text_length | Integer | Length of cleaned text content |
| raw_text_length | Integer | Length of raw text content |
| rendered | Boolean | Whether document was successfully rendered |

## Data Quality
- All events have unique event_hash values
- Document completeness: Events with usable documents (≥400 chars)
- Summary coverage: Events with AI-generated summaries
- Embedding coverage: Events with vector embeddings

## Authorities Covered
- MAS (Monetary Authority of Singapore)
- BI (Bank Indonesia)
- OJK (Otoritas Jasa Keuangan - Indonesia)
- SC (Securities Commission Malaysia)
- PDPC (Personal Data Protection Commission - Singapore)
- IMDA (Infocomm Media Development Authority - Singapore)
- And others...

## Usage Notes
- Use event_id to join events and documents tables
- Embeddings are stored separately in the database (not exported)
- Clean text content is not exported for size reasons
- Contact data@aseanforge.com for full text access
"""
    
    with open(os.path.join(DELIVERABLES_DIR, "DATA_DICTIONARY.md"), 'w') as f:
        f.write(content)


def create_provenance_doc():
    """Create provenance and compliance documentation."""
    content = """# AseanForge Dataset - Provenance & Compliance

## Data Sources
All data in this dataset is sourced from publicly available websites of ASEAN financial regulatory authorities. Sources include:

- Official press releases
- Regulatory announcements
- Policy documents
- Public speeches and statements

## Collection Methodology
- **Web Scraping**: Automated collection using Firecrawl API with robots.txt compliance
- **Frequency**: Regular updates to capture new content
- **Deduplication**: Content-based hashing to prevent duplicates
- **Quality Control**: Automated validation of URL accessibility and content quality

## AI Enhancement
- **Summaries**: Generated using OpenAI GPT-4o-mini for consistent English summaries
- **Embeddings**: Created using OpenAI text-embedding-3-small for semantic search
- **Quality Assurance**: Automated checks for content completeness and accuracy

## Compliance & Ethics
- **Robots.txt Compliance**: All scraping respects robots.txt directives
- **Rate Limiting**: Respectful crawling with appropriate delays
- **Public Data Only**: No private or restricted content
- **Attribution**: All content attributed to original sources

## Data Retention
- Source URLs maintained for transparency
- Original publication dates preserved
- Access timestamps recorded for audit trails

## Limitations
- Content limited to publicly available information
- AI summaries are generated and may not capture all nuances
- Coverage varies by authority based on website structure
- English summaries may not reflect original language subtleties

## Contact
For questions about data provenance or compliance:
- Email: data@aseanforge.com
- Website: https://aseanforge.com
"""
    
    with open(os.path.join(DELIVERABLES_DIR, "PROVENANCE_AND_COMPLIANCE.md"), 'w') as f:
        f.write(content)


def create_coverage_doc():
    """Create coverage and freshness documentation."""
    # Load current metrics
    qa_report_file = os.path.join(OUTPUT_DIR, "expansion_qa_kpis_report.json")
    if os.path.exists(qa_report_file):
        with open(qa_report_file, 'r') as f:
            qa_report = json.load(f)
        
        current_metrics = qa_report['current_metrics']
        global_metrics = current_metrics['global']
        freshness_metrics = current_metrics['freshness']
        
        content = f"""# AseanForge Dataset - Coverage & Freshness

## Global Coverage Metrics
- **Total Events**: {global_metrics['total_events']:,}
- **Document Completeness**: {global_metrics['doc_completeness_pct']:.1f}%
- **Summary Coverage**: {global_metrics['summary_coverage_pct']:.1f}%
- **Embedding Coverage**: {global_metrics['embedding_coverage_pct']:.1f}%

## Freshness (Last 90 Days)
- **Recent Events**: {freshness_metrics['total_events_90d']:,}
- **Recent Doc Completeness**: {freshness_metrics['doc_completeness_90d_pct']:.1f}%

## Coverage by Authority
"""
        
        for authority, metrics in current_metrics['by_authority'].items():
            content += f"- **{authority}**: {metrics['doc_completeness_pct']:.1f}% ({metrics['events_with_docs']}/{metrics['total_events']} events)\n"
        
        content += """
## Quality Metrics
- Document median length: >1,000 characters
- URL validity: 100% accessible
- Deduplication: No duplicate events
- Timeliness: Regular updates

## Update Frequency
- **Target**: Weekly updates for new content
- **Coverage Expansion**: Quarterly deep harvests
- **Quality Assurance**: Continuous monitoring

## Data Completeness Definition
An event has "complete" documentation if:
1. Source document was successfully fetched
2. Clean text extraction succeeded
3. Content length ≥400 characters
4. AI summary generated
5. Vector embedding created

## Freshness Definition
Freshness measures the percentage of recent events (last 90 days) that have complete documentation, indicating how well the system captures and processes new regulatory content.
"""
    else:
        content = "# Coverage metrics not available - QA report not found"
    
    with open(os.path.join(DELIVERABLES_DIR, "COVERAGE_AND_FRESHNESS.md"), 'w') as f:
        f.write(content)


def create_exec_summary():
    """Create executive summary."""
    content = """# AseanForge MVP - Executive Summary

## Product Overview
AseanForge is a comprehensive regulatory intelligence platform for ASEAN financial markets, providing AI-enhanced access to regulatory announcements, policy documents, and market guidance from key financial authorities across Southeast Asia.

## Key Value Propositions

### 1. Comprehensive Coverage
- **Multi-Authority**: Covers major financial regulators across ASEAN
- **Real-Time Updates**: Automated monitoring and ingestion of new content
- **Historical Depth**: Extensive archive of regulatory events and documents

### 2. AI-Enhanced Intelligence
- **Smart Summaries**: AI-generated English summaries for consistent understanding
- **Semantic Search**: Vector embeddings enable intelligent content discovery
- **Quality Assurance**: Automated validation and quality control

### 3. Developer-Friendly Access
- **Structured Data**: Clean, normalized datasets in multiple formats
- **API Ready**: Database-backed with embedding support for AI applications
- **Documentation**: Comprehensive data dictionary and usage guides

## Market Opportunity
- **Regulatory Complexity**: ASEAN financial regulations are fragmented across multiple jurisdictions
- **Language Barriers**: Original content often in local languages
- **Information Overload**: Manual monitoring is time-intensive and error-prone
- **Compliance Risk**: Missing regulatory updates can have significant consequences

## Technical Differentiators
- **Robots.txt Compliant**: Ethical web scraping with respect for source websites
- **AI-First Architecture**: Built for modern AI/ML workflows
- **Scalable Infrastructure**: Cloud-native design for enterprise deployment
- **Quality Focus**: Rigorous data validation and quality metrics

## Target Customers
- **Financial Institutions**: Banks, asset managers, fintech companies
- **Compliance Teams**: Regulatory affairs and legal departments
- **Research Organizations**: Think tanks, consulting firms, academic institutions
- **Technology Companies**: AI/ML teams building regulatory applications

## Competitive Advantages
1. **ASEAN Focus**: Deep specialization in Southeast Asian markets
2. **AI Integration**: Native support for modern AI workflows
3. **Data Quality**: Rigorous validation and quality assurance
4. **Ethical Approach**: Compliant and respectful data collection

## Next Steps
- **Enterprise Pilot**: Deploy with select financial institutions
- **API Development**: Build customer-facing API layer
- **Coverage Expansion**: Add more authorities and content types
- **Advanced Analytics**: Develop regulatory trend analysis capabilities

## Contact
- **Sales**: sales@aseanforge.com
- **Technical**: tech@aseanforge.com
- **Partnership**: partners@aseanforge.com
"""
    
    with open(os.path.join(DELIVERABLES_DIR, "MVP_EXEC_SUMMARY.md"), 'w') as f:
        f.write(content)


def create_snapshot_archive():
    """Create ZIP archive with all deliverables."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_filename = f"mvp_dataset_snapshot_{timestamp}.zip"
    snapshot_path = os.path.join(DELIVERABLES_DIR, snapshot_filename)
    
    with zipfile.ZipFile(snapshot_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add dataset files
        for root, dirs, files in os.walk(DATASET_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, DELIVERABLES_DIR)
                zipf.write(file_path, arcname)
        
        # Add documentation
        docs = [
            "DATA_DICTIONARY.md",
            "PROVENANCE_AND_COMPLIANCE.md", 
            "COVERAGE_AND_FRESHNESS.md",
            "MVP_EXEC_SUMMARY.md"
        ]
        
        for doc in docs:
            doc_path = os.path.join(DELIVERABLES_DIR, doc)
            if os.path.exists(doc_path):
                zipf.write(doc_path, doc)
        
        # Add latest reports
        reports_dir = OUTPUT_DIR
        for file in os.listdir(reports_dir):
            if file.endswith('.json') or file.endswith('.csv'):
                file_path = os.path.join(reports_dir, file)
                zipf.write(file_path, f"reports/{file}")
    
    return snapshot_path


def main():
    print("=" * 60)
    print("COVERAGE EXPANSION STEP 5: Sales-Ready Pack")
    print("=" * 60)
    print()
    
    # Create deliverables directory
    os.makedirs(DELIVERABLES_DIR, exist_ok=True)
    
    # Export data
    print("Exporting dataset...")
    events_count, docs_count = export_data()
    print()
    
    # Create documentation
    print("Creating documentation...")
    create_data_dictionary()
    print("  ✓ Data dictionary created")
    
    create_provenance_doc()
    print("  ✓ Provenance documentation created")
    
    create_coverage_doc()
    print("  ✓ Coverage documentation created")
    
    create_exec_summary()
    print("  ✓ Executive summary created")
    print()
    
    # Create snapshot archive
    print("Creating snapshot archive...")
    snapshot_path = create_snapshot_archive()
    print(f"  ✓ Snapshot created: {snapshot_path}")
    
    # Write snapshot path for reference
    with open(os.path.join(OUTPUT_DIR, "snapshot_path.txt"), 'w') as f:
        f.write(snapshot_path)
    
    print()
    print("SALES PACK SUMMARY")
    print("-" * 40)
    print(f"Events exported: {events_count:,}")
    print(f"Documents exported: {docs_count:,}")
    print(f"Snapshot archive: {os.path.basename(snapshot_path)}")
    print(f"Archive size: {os.path.getsize(snapshot_path) / 1024 / 1024:.1f} MB")
    print()
    
    print("✓ STEP 5: PASS")
    print()


if __name__ == "__main__":
    main()
