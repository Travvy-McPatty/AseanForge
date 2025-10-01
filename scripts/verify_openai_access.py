#!/usr/bin/env python3
"""
Verify OpenAI API Access

Tests API key validity and queries current usage/limits.

Usage:
    .venv/bin/python scripts/verify_openai_access.py
"""

import json
import os
import sys
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv("app/.env")
except Exception:
    pass

try:
    import openai
except ImportError:
    print("ERROR: openai library not installed. Run: pip install openai", file=sys.stderr)
    sys.exit(1)


def main():
    output_dir = "data/output/validation/latest"
    os.makedirs(output_dir, exist_ok=True)
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in app/.env", file=sys.stderr)
        sys.exit(1)
    
    print("=== OpenAI API Pre-Flight Check ===")
    print(f"API Key: {api_key[:8]}...{api_key[-4:]}")
    print()
    
    # Initialize client
    client = openai.OpenAI(api_key=api_key)
    
    # Test 1: Simple completion request
    print("Test 1: Simple completion request...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Say 'OK' if you can read this."}],
            max_tokens=5
        )
        
        result = response.choices[0].message.content
        print(f"  ✓ Response: {result}")
        print(f"  ✓ Model: {response.model}")
        print(f"  ✓ Tokens: {response.usage.total_tokens}")
    except Exception as e:
        print(f"  ✗ FAILED: {e}", file=sys.stderr)
        sys.exit(1)
    
    print()
    
    # Test 2: List models (verify access to required models)
    print("Test 2: Verify required models...")
    try:
        models = client.models.list()
        model_ids = [m.id for m in models.data]
        
        required_models = ["gpt-4o-mini", "text-embedding-3-small"]
        for model in required_models:
            if model in model_ids:
                print(f"  ✓ {model} available")
            else:
                print(f"  ⚠ {model} not found in model list (may still work)")
    except Exception as e:
        print(f"  ⚠ Could not list models: {e}")
    
    print()
    
    # Test 3: Query usage (if available)
    print("Test 3: Query usage/limits...")
    usage_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_key_prefix": api_key[:8],
        "test_completion_success": True,
        "note": "OpenAI API does not expose usage/limits via API; check dashboard manually"
    }
    
    # Save usage data
    output_path = os.path.join(output_dir, "openai_usage_start.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(usage_data, f, indent=2)
    
    print(f"  ✓ Usage data saved to: {output_path}")
    print()
    
    # Test 4: Verify Batch API access
    print("Test 4: Verify Batch API access...")
    try:
        # List existing batches (should return empty list or existing batches)
        batches = client.batches.list(limit=1)
        print(f"  ✓ Batch API accessible")
        print(f"  ✓ Found {len(batches.data)} existing batch(es)")
    except Exception as e:
        print(f"  ✗ FAILED: {e}", file=sys.stderr)
        print("  Batch API may not be available for this account", file=sys.stderr)
        sys.exit(1)
    
    print()
    print("=== Pre-Flight Check Complete ===")
    print("✓ OpenAI API key valid")
    print("✓ Completion API working")
    print("✓ Batch API accessible")
    print()


if __name__ == "__main__":
    main()

