import os
import json
import time
from typing import Dict, Optional, Set

# Pricing per 1M tokens (USD). Keep this updated with OpenAI pricing.
# Defaults come from public pricing pages where available; Deep Research tiers are env-configurable.
# Env vars (USD per 1M tokens):
#   PRICE_O4_MINI_DR_INPUT, PRICE_O4_MINI_DR_OUTPUT
#   PRICE_O3_DR_INPUT,       PRICE_O3_DR_OUTPUT
PRICE_O4_MINI_DR_INPUT = float(os.getenv("PRICE_O4_MINI_DR_INPUT", "0") or 0)
PRICE_O4_MINI_DR_OUTPUT = float(os.getenv("PRICE_O4_MINI_DR_OUTPUT", "0") or 0)
PRICE_O3_DR_INPUT = float(os.getenv("PRICE_O3_DR_INPUT", "0") or 0)
PRICE_O3_DR_OUTPUT = float(os.getenv("PRICE_O3_DR_OUTPUT", "0") or 0)

PRICING_PER_1M: Dict[str, Dict[str, float]] = {
    "gpt-4o-mini": {"input": 0.60, "output": 2.40},
    "text-embedding-3-small": {"input": 0.02, "output": 0.0},
    # Deep Research tiers (configurable via env; default to 0 to avoid overestimating)
    "o4-mini-deep-research": {"input": PRICE_O4_MINI_DR_INPUT, "output": PRICE_O4_MINI_DR_OUTPUT},
    "o3-deep-research": {"input": PRICE_O3_DR_INPUT, "output": PRICE_O3_DR_OUTPUT},
}

STAGES = ["research_queries", "content_processing", "report_generation"]


class TokenTracker:
    """
    Minimal token/cost tracker for a single run.
    - Call record(model, stage, input_tokens, output_tokens) after each LLM/embeddings call
    - At end, call summary_dict() for structured data and pretty_summary() for console
    """

    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or str(int(time.time()))
        self.models_used: Set[str] = set()
        self.totals_by_model: Dict[str, Dict[str, int]] = {}
        self.totals_by_stage: Dict[str, Dict[str, int]] = {
            s: {"input": 0, "output": 0} for s in STAGES
        }

    def record(self, model: str, stage: str, input_tokens: int = 0, output_tokens: int = 0):
        stage = stage if stage in self.totals_by_stage else "content_processing"
        self.models_used.add(model)
        if model not in self.totals_by_model:
            self.totals_by_model[model] = {"input": 0, "output": 0}
        self.totals_by_model[model]["input"] += int(input_tokens or 0)
        self.totals_by_model[model]["output"] += int(output_tokens or 0)
        self.totals_by_stage[stage]["input"] += int(input_tokens or 0)
        self.totals_by_stage[stage]["output"] += int(output_tokens or 0)

    def total_input(self) -> int:
        return sum(m["input"] for m in self.totals_by_model.values())

    def total_output(self) -> int:
        return sum(m["output"] for m in self.totals_by_model.values())

    def _cost_for_model(self, model: str) -> Dict[str, float]:
        pricing = PRICING_PER_1M.get(model)
        usage = self.totals_by_model.get(model, {"input": 0, "output": 0})
        if not pricing:
            return {"input_usd": 0.0, "output_usd": 0.0, "total_usd": 0.0, "unknown_pricing": True}
        input_cost = (usage["input"] / 1_000_000.0) * pricing.get("input", 0.0)
        output_cost = (usage["output"] / 1_000_000.0) * pricing.get("output", 0.0)
        return {
            "input_usd": round(input_cost, 6),
            "output_usd": round(output_cost, 6),
            "total_usd": round(input_cost + output_cost, 6),
            "unknown_pricing": False,
        }

    def cost_breakdown(self) -> Dict[str, Dict[str, float]]:
        return {m: self._cost_for_model(m) for m in self.models_used}

    def total_cost_usd(self) -> float:
        return round(sum(b["total_usd"] for b in self.cost_breakdown().values()), 6)

    def summary_dict(self) -> Dict:
        return {
            "event": "usage_summary",
            "run_id": self.run_id,
            "models_used": sorted(self.models_used),
            "tokens": {
                "input": self.total_input(),
                "output": self.total_output(),
                "by_stage": self.totals_by_stage,
                "by_model": self.totals_by_model,
            },
            "cost": {
                "total_usd": self.total_cost_usd(),
                "by_model": self.cost_breakdown(),
            },
        }

    def json_line(self) -> str:
        return json.dumps(self.summary_dict(), ensure_ascii=False)

    def pretty_summary(self) -> str:
        return (
            f"Tokens used: {self.total_input()} input, {self.total_output()} output. "
            f"Estimated cost: ${self.total_cost_usd():.4f}"
        )

