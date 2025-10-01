#!/usr/bin/env python3
"""
Batch Enrichment CLI

Command-line interface for manual batch control.

Usage:
    # Build request files
    .venv/bin/python -m app.enrich_batch.cli build --kind embeddings --since 2025-07-01 --limit 20000 --out data/batch/embeddings.requests.jsonl
    .venv/bin/python -m app.enrich_batch.cli build --kind summaries --since 2025-07-01 --limit 10000 --out data/batch/summaries.requests.jsonl
    
    # Submit batches
    .venv/bin/python -m app.enrich_batch.cli submit --kind embeddings --input data/batch/embeddings.requests.jsonl
    .venv/bin/python -m app.enrich_batch.cli submit --kind summaries --input data/batch/summaries.requests.jsonl
    
    # Poll batch status
    .venv/bin/python -m app.enrich_batch.cli poll --batch-id batch_abc123
    
    # Merge results to DB
    .venv/bin/python -m app.enrich_batch.cli merge --kind embeddings --batch-id batch_abc123
    .venv/bin/python -m app.enrich_batch.cli merge --kind summaries --batch-id batch_def456
    
    # Get batch status
    .venv/bin/python -m app.enrich_batch.cli status --batch-id batch_abc123
    
    # Cancel batch
    .venv/bin/python -m app.enrich_batch.cli cancel --batch-id batch_abc123
"""

import argparse
import json
import sys

from . import builders, submit, poll, merge


def cmd_build(args):
    """Build JSONL request file."""
    # Parse authorities
    authorities = None
    if args.authorities:
        authorities = [a.strip().upper() for a in args.authorities.split(",")]

    if args.kind == "embeddings":
        metadata = builders.build_embedding_requests(
            since_date=args.since,
            limit=args.limit,
            output_path=args.out,
            authorities=authorities
        )
    elif args.kind == "summaries":
        metadata = builders.build_summary_requests(
            since_date=args.since,
            limit=args.limit,
            output_path=args.out,
            authorities=authorities
        )
    else:
        print(f"ERROR: Invalid kind '{args.kind}'. Must be 'embeddings' or 'summaries'", file=sys.stderr)
        sys.exit(1)
    
    # Print metadata as JSON
    print("\n" + json.dumps(metadata, indent=2))
    
    # Check budget
    budget = float(args.budget) if args.budget else None
    if budget and metadata.get("projected_cost_usd", 0) > budget:
        print(f"\nWARNING: Projected cost ${metadata['projected_cost_usd']:.4f} exceeds budget ${budget:.2f}", file=sys.stderr)
        sys.exit(1)


def cmd_submit(args):
    """Submit batch job."""
    batch_id = submit.submit_batch(
        input_file_path=args.input,
        kind=args.kind
    )
    
    print(f"\nBatch ID: {batch_id}")
    print(f"\nTo poll status:")
    print(f"  .venv/bin/python -m app.enrich_batch.cli poll --batch-id {batch_id}")


def cmd_poll(args):
    """Poll batch status until completion."""
    result = poll.poll_batch(
        batch_id=args.batch_id,
        poll_interval_seconds=args.interval,
        timeout_hours=args.timeout
    )
    
    print("\n" + json.dumps(result, indent=2))
    
    if result["status"] == "completed":
        print(f"\n✓ Batch completed successfully")
        print(f"\nTo merge results:")
        print(f"  .venv/bin/python -m app.enrich_batch.cli merge --kind <embeddings|summaries> --batch-id {args.batch_id}")
    else:
        print(f"\n✗ Batch did not complete: {result['status']}", file=sys.stderr)
        sys.exit(1)


def cmd_merge(args):
    """Merge results to database."""
    # Determine results file path
    if args.results:
        results_path = args.results
    else:
        results_path = f"data/batch/{args.batch_id}.results.jsonl"
    
    if args.kind == "embeddings":
        stats = merge.merge_embeddings(results_path)
    elif args.kind == "summaries":
        stats = merge.merge_summaries(results_path)
    else:
        print(f"ERROR: Invalid kind '{args.kind}'. Must be 'embeddings' or 'summaries'", file=sys.stderr)
        sys.exit(1)
    
    print("\n" + json.dumps(stats, indent=2))


def cmd_status(args):
    """Get batch status."""
    status = submit.get_batch_status(args.batch_id)
    print(json.dumps(status, indent=2))


def cmd_cancel(args):
    """Cancel batch job."""
    success = poll.cancel_batch(args.batch_id)
    if success:
        print(f"✓ Batch {args.batch_id} cancelled")
    else:
        print(f"✗ Failed to cancel batch {args.batch_id}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="OpenAI Batch API Enrichment CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to run")
    
    # Build command
    p_build = subparsers.add_parser("build", help="Build JSONL request file")
    p_build.add_argument("--kind", required=True, choices=["embeddings", "summaries"], help="Request type")
    p_build.add_argument("--since", help="Filter events since date (YYYY-MM-DD)")
    p_build.add_argument("--limit", type=int, help="Maximum number of items to process")
    p_build.add_argument("--out", required=True, help="Output JSONL file path")
    p_build.add_argument("--authorities", help="Comma-separated list of authority codes (e.g., MAS,IMDA)")
    p_build.add_argument("--budget", type=float, help="Cost budget in USD (abort if exceeded)")
    p_build.set_defaults(func=cmd_build)
    
    # Submit command
    p_submit = subparsers.add_parser("submit", help="Submit batch job")
    p_submit.add_argument("--kind", required=True, choices=["embeddings", "summaries"], help="Request type")
    p_submit.add_argument("--input", required=True, help="Input JSONL file path")
    p_submit.set_defaults(func=cmd_submit)
    
    # Poll command
    p_poll = subparsers.add_parser("poll", help="Poll batch status until completion")
    p_poll.add_argument("--batch-id", required=True, help="Batch ID")
    p_poll.add_argument("--interval", type=int, default=60, help="Poll interval in seconds (default: 60)")
    p_poll.add_argument("--timeout", type=int, default=26, help="Timeout in hours (default: 26)")
    p_poll.set_defaults(func=cmd_poll)
    
    # Merge command
    p_merge = subparsers.add_parser("merge", help="Merge results to database")
    p_merge.add_argument("--kind", required=True, choices=["embeddings", "summaries"], help="Request type")
    p_merge.add_argument("--batch-id", required=True, help="Batch ID")
    p_merge.add_argument("--results", help="Results JSONL file path (default: data/batch/<batch_id>.results.jsonl)")
    p_merge.set_defaults(func=cmd_merge)
    
    # Status command
    p_status = subparsers.add_parser("status", help="Get batch status")
    p_status.add_argument("--batch-id", required=True, help="Batch ID")
    p_status.set_defaults(func=cmd_status)
    
    # Cancel command
    p_cancel = subparsers.add_parser("cancel", help="Cancel batch job")
    p_cancel.add_argument("--batch-id", required=True, help="Batch ID")
    p_cancel.set_defaults(func=cmd_cancel)
    
    args = parser.parse_args()
    
    # Execute command
    args.func(args)


if __name__ == "__main__":
    main()

