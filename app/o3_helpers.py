#!/usr/bin/env python3
"""
Helpers for calling o3-deep-research via the OpenAI Responses API.
Returns a normalized dict: {"text": str, "sources": list, "usage": dict}
"""
from __future__ import annotations
import os, json, time
from typing import Dict, Any

try:
    from openai import OpenAI
    _client = OpenAI()
except Exception:
    _client = None

def _extract_text(resp) -> str:
    # Prefer .output_text if available
    txt = getattr(resp, "output_text", None)
    if txt:
        return txt
    # Fallback: inspect .output / .response / choices structures
    for attr in ("output", "response", "choices"):
        obj = getattr(resp, attr, None)
        if not obj:
            continue
        try:
            if isinstance(obj, list):
                # Try common structures
                for item in obj:
                    if isinstance(item, dict):
                        if item.get("type") == "message":
                            # responses format
                            parts = item.get("content") or []
                            return "\n".join(p.get("text","") for p in parts if isinstance(p, dict))
                        if item.get("message"):
                            return str(item.get("message"))
                    elif hasattr(item, "message") and getattr(item.message, "content", None):
                        return item.message.content
            # Dict-like
            if isinstance(obj, dict):
                return obj.get("message") or obj.get("content") or ""
        except Exception:
            continue
    # Last resort: JSON dump
    try:
        return json.dumps(resp, ensure_ascii=False)
    except Exception:
        return ""


def call_o3_deep_research(prompt: str) -> Dict[str, Any]:
    if not _client:
        return {"text": "[o3 unavailable] " + prompt, "sources": [], "usage": {}}
    try:
        resp = _client.responses.create(model="o3-deep-research", input=prompt)
        text = _extract_text(resp) or ""
        usage = getattr(resp, "usage", None)
        if usage and hasattr(usage, "model"):  # pydantic-like objects
            usage = {
                "input_tokens": getattr(usage, "input_tokens", 0) or 0,
                "output_tokens": getattr(usage, "output_tokens", 0) or 0,
                "total_tokens": getattr(usage, "total_tokens", 0) or 0,
            }
        elif isinstance(usage, dict):
            usage = {
                "input_tokens": usage.get("input_tokens", 0) or 0,
                "output_tokens": usage.get("output_tokens", 0) or 0,
                "total_tokens": usage.get("total_tokens", 0) or 0,
            }
        else:
            usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        # Deep Research sources may appear in annotations; keep placeholder empty for now
        return {"text": text, "sources": [], "usage": usage}
    except Exception as e:
        # Log-style return
        return {"text": f"[o3_error] {e}", "sources": [], "usage": {}}

