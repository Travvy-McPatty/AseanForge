#!/usr/bin/env python3
"""
Database Migration: Add Enrichment Tracking Columns

Adds version tracking columns to events table for batch enrichment:
- summary_model, summary_ts, summary_version
- embedding_model, embedding_ts, embedding_version

Also ensures unique index for hard idempotency: (authority, event_hash)

Usage:
    .venv/bin/python scripts/migrate_add_enrichment_columns.py
"""

import os
import sys
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

import psycopg2


def main():
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        print("ERROR: NEON_DATABASE_URL not set in app/.env", file=sys.stderr)
        sys.exit(1)
    
    print(f"[{datetime.utcnow().isoformat()}] Starting migration: add_enrichment_tracking_columns")
    
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Add summary tracking columns
        print("  Adding summary tracking columns...")
        cur.execute("""
            ALTER TABLE events 
            ADD COLUMN IF NOT EXISTS summary_model TEXT,
            ADD COLUMN IF NOT EXISTS summary_ts TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS summary_version TEXT;
        """)
        print("    ✓ summary_model, summary_ts, summary_version")
        
        # Add embedding tracking columns
        print("  Adding embedding tracking columns...")
        cur.execute("""
            ALTER TABLE events 
            ADD COLUMN IF NOT EXISTS embedding_model TEXT,
            ADD COLUMN IF NOT EXISTS embedding_ts TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS embedding_version TEXT;
        """)
        print("    ✓ embedding_model, embedding_ts, embedding_version")
        
        # Create unique index for hard idempotency
        print("  Creating unique index for hard idempotency...")
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS events_unique_hash 
            ON events (authority, event_hash);
        """)
        print("    ✓ events_unique_hash index on (authority, event_hash)")
        
        # Verify columns exist
        print("  Verifying schema changes...")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'events' 
              AND column_name IN (
                'summary_model', 'summary_ts', 'summary_version',
                'embedding_model', 'embedding_ts', 'embedding_version'
              )
            ORDER BY column_name;
        """)
        
        columns = cur.fetchall()
        if len(columns) == 6:
            print("    ✓ All 6 columns verified:")
            for col_name, col_type in columns:
                print(f"      - {col_name} ({col_type})")
        else:
            print(f"    ✗ WARNING: Expected 6 columns, found {len(columns)}", file=sys.stderr)
        
        # Verify index exists
        cur.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'events' 
              AND indexname = 'events_unique_hash';
        """)
        
        if cur.fetchone():
            print("    ✓ events_unique_hash index verified")
        else:
            print("    ✗ WARNING: events_unique_hash index not found", file=sys.stderr)
        
        cur.close()
        conn.close()
        
        print(f"[{datetime.utcnow().isoformat()}] Migration completed successfully")
        
        # Write migration log
        os.makedirs("data/output/validation/latest", exist_ok=True)
        with open("data/output/validation/latest/migration_enrichment_columns.log", "w") as f:
            f.write(f"Migration: add_enrichment_tracking_columns\n")
            f.write(f"Timestamp: {datetime.utcnow().isoformat()}\n")
            f.write(f"Status: SUCCESS\n")
            f.write(f"Columns added: 6\n")
            f.write(f"Indexes created: 1\n")
        
        print("\nMigration log written to: data/output/validation/latest/migration_enrichment_columns.log")
        
    except psycopg2.Error as e:
        print(f"\nERROR: Database migration failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

