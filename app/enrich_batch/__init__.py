"""
OpenAI Batch API Enrichment Module

Provides async batch processing for summaries and embeddings at ~50% cost savings.

Modules:
- builders: Build JSONL request files
- submit: Upload to OpenAI Files API and create batch jobs
- poll: Poll batch status until completion
- merge: Parse results and upsert to database
- cli: Command-line interface
"""

__version__ = "1.0.0"

