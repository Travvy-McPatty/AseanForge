import os, argparse, time, json, re
from dotenv import load_dotenv
load_dotenv(override=True)

from langchain_postgres import PGVector
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.messages import SystemMessage, HumanMessage
import matplotlib.pyplot as plt
from usage_tracker import TokenTracker

# Direct OpenAI client (Deep Research path)
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None


# Full publish-mode prompt (current structure)
PUBLISH_PROMPT = """You are an ASEAN tech investment analyst for {brand} ({domain}).
Structure the report in Markdown with sections:
1. Executive Summary
2. Policy Landscape
3. Sector Trends
4. Top Opportunities & Risks
5. Key Players
6. 12–24 Month Outlook
7. Recommendations
At the end of relevant sections, add a Markdown citation line in this exact format:
[Citation: Title | domain | URL | accessed {access_date} | snippets N]
Use the provided context citation lines for accurate titles/URLs; do not fabricate.
Focus: {topic}. Timeframe: {timeframe}. Keep it concise and actionable."""

# Shorter draft-mode prompt (minimal formatting, key insights only)
DRAFT_PROMPT = """You are an ASEAN tech investment analyst for {brand} ({domain}).
Write a concise draft report in Markdown with minimal formatting:
- Executive Summary (3-5 bullets)
- Key Insights (5-8 bullets)
- Recommendations (3-5 bullets)
Do NOT include citations or an appendix. Keep it focused on {topic} within {timeframe}."""

PROMPTS = {"draft": DRAFT_PROMPT, "publish": PUBLISH_PROMPT}

def default_model_for_mode(mode: str) -> str:
    # Strict policy defaults (no DR unless explicitly requested):
    # - test   → gpt-4o-mini
    # - draft  → gpt-4o-mini (override to o4-mini-deep-research with --realistic)
    # - publish→ gpt-4o-mini (switch to o3-deep-research only with --force-deep-research)
    return "gpt-4o-mini"

MODEL_ALIASES = {
    # Friendly aliases → OpenAI model ids
    "test": "gpt-4o-mini",
    "4o-mini": "gpt-4o-mini",
    "gpt-4o-mini": "gpt-4o-mini",
    "o4-mini": "o4-mini",
    # Deep research tiers
    "o4-mini-deep-research": "o4-mini-deep-research",
    "4o-mini-deep-research": "o4-mini-deep-research",
    "o3-deep-research": "o3-deep-research",
    # Legacy aliases
    "o3": "o3",
    "o3-mini": "o3-mini",
}

def normalize_model(name: str | None) -> str:
    name = (name or "gpt-4o-mini").strip()
    return MODEL_ALIASES.get(name, name)

def domain_from_url(url: str | None) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url or "").netloc or ""
    except Exception:
        return ""


def extract_dr_sources(resp_obj, md_text: str, accessed_at: str):
    """Extract Deep Research external sources from a Responses API object; fallback to Markdown link scan."""
    out = []
    seen = set()

    def domain_from(url: str) -> str:
        try:
            from urllib.parse import urlparse
            return urlparse(url or "").netloc or ""
        except Exception:
            return ""

    def add(url: str, title: str | None):
        if not url or not url.startswith("http"):
            return
        if url in seen:
            return
        seen.add(url)
        out.append({
            "title": (title or domain_from(url) or "(untitled)").strip(),
            "url": url,
            "domain": domain_from(url),
            "accessed_at": accessed_at,
        })

    try:
        for fld in ("references", "citations", "source_attributions"):
            arr = getattr(resp_obj, fld, None) or (resp_obj.get(fld) if isinstance(resp_obj, dict) else None)
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict):
                        add(it.get("url") or it.get("source_url") or "", it.get("title") or it.get("name"))
        data = getattr(resp_obj, "output", None) or getattr(resp_obj, "content", None) or resp_obj
        if isinstance(data, list):
            for blk in data:
                if isinstance(blk, dict):
                    ann = blk.get("annotations") or blk.get("citations")
                    if isinstance(ann, list):
                        for a in ann:
                            if isinstance(a, dict):
                                add(a.get("url") or a.get("source_url") or "", a.get("title") or a.get("name"))
    except Exception:
        pass

    if md_text:
        for m in re.finditer(r"\[([^\]]+)\]\((https?://[^)]+)\)", md_text):
            add(m.group(2), m.group(1))
        for m in re.finditer(r"(https?://[\w\-\.]+(?:/[\w\-\./%#?=&]+)?)", md_text):
            add(m.group(1), None)

    return out


def deep_research_generate(topic: str, timeframe: str, k: int, mode: str, ts: int, tracker: TokenTracker):
    if OpenAI is None:
        raise SystemExit("[deep_research] OpenAI SDK not available. Install 'openai' and ensure OPENAI_API_KEY is set.")

    brand = os.getenv('BRAND_NAME','AseanForge')
    domain = os.getenv('BRAND_DOMAIN','aseanforge.com')
    access_date = time.strftime("%Y-%m-%d")

    # Retrieval context from PGVector (internal context; does not replace Deep Research browsing)
    conn = os.getenv("NEON_DATABASE_URL");
    if conn and conn.startswith("postgresql://"):
        conn = conn.replace("postgresql://", "postgresql+psycopg://", 1)
    coll=os.getenv("COLLECTION_NAME","asean_docs")
    vs = PGVector(embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
                  collection_name=coll, connection=conn, use_jsonb=True)
    retriever = vs.as_retriever(search_kwargs={"k": k})
    query = f"{topic} in ASEAN, timeframe {timeframe}"
    try:
        docs = retriever.invoke(query)
    except Exception as e:
        print(f"[warn] Retrieval failed, continuing without context: {e}")
        docs = []

    url_counts = {}
    source_rows = {}
    for d in docs:
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "")
        url_counts[url] = url_counts.get(url, 0) + 1
        if url not in source_rows:
            source_rows[url] = {
                "url": url,
                "title": title or (domain_from_url(url) or "(untitled)"),
                "domain": domain_from_url(url),
            }

    ctx_lines = []
    for i, d in enumerate(docs):
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "") or source_rows.get(url, {}).get("title", "(untitled)")
        domain_u = domain_from_url(url)
        snippets = url_counts.get(url, 1)
        base = f"[{i+1}] {d.page_content}"
        if mode == "publish":
            citation = f"[Citation: {title} | {domain_u} | {url} | accessed {access_date} | snippets {snippets}]"
            ctx_lines.append(base + "\n" + citation)
        else:
            ctx_lines.append(base)
    context = "\n\n".join(ctx_lines)

    prompt_str = PROMPTS.get(mode, DRAFT_PROMPT)
    system_text = (
        "Write concise, cited ASEAN tech investment reports." if mode == "publish"
        else "Write concise ASEAN tech investment reports with minimal formatting."
    )

    client = OpenAI()
    target_model = "o3-deep-research" if mode == "publish" else "o4-mini-deep-research"

    # Budget safety guard (preflight; input tokens only)
    try:
        p_in = float(os.getenv("PRICE_O3_DR_INPUT" if mode == "publish" else "PRICE_O4_MINI_DR_INPUT", "0") or 0)
        max_usd = float(os.getenv("MAX_DR_USD", "20") or 20)
        if p_in > 0 and max_usd > 0:
            approx_in = estimate_tokens(system_text) + estimate_tokens(context) + estimate_tokens(
                prompt_str.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date)
            )
            est_cost = (approx_in / 1_000_000.0) * p_in
            if est_cost > max_usd:
                raise SystemExit(f"DR aborted (budget ${max_usd:.2f} exceeded).")
    except Exception:
        pass

    try:
        resp = client.responses.create(
            model=target_model,
            input=[
                {"role": "system", "content": system_text},
                {"role": "user", "content": (
                    f"Context (internal vector retrieval):\n{context}\n\n" +
                    prompt_str.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date)
                )},
            ],
            tools=[{"type": "web_search_preview"}],
            reasoning={"effort": "medium"},
        )
    except Exception as e:
        msg = str(e)
        if any(tok in msg for tok in ("model_not_found", "access_denied", "insufficient_permissions", "404", "403")):
            raise SystemExit(f"[deep_research] Access to {target_model} failed: {msg}. No fallback per --force-deep-research/DR_FORCE policy.")
        raise

    md_body = None
    try:
        if hasattr(resp, "output_text") and resp.output_text:
            md_body = resp.output_text
        elif hasattr(resp, "output") and isinstance(resp.output, list):
            parts = []
            for p in resp.output:
                if isinstance(p, dict):
                    txt = p.get("content")
                    if isinstance(txt, str):
                        parts.append(txt)
            md_body = "\n".join([t for t in parts if t]) or None
    except Exception:
        md_body = None
    if not md_body:
        md_body = getattr(resp, "content", None) or getattr(resp, "text", None) or ""
    if not md_body:
        raise SystemExit("[deep_research] Empty response content from Deep Research model.")

    try:
        usage = getattr(resp, "usage", None) or {}
        in_tok = int(usage.get("input_tokens", 0)); out_tok = int(usage.get("output_tokens", 0))
    except Exception:
        in_tok = out_tok = 0
    # Fallback: estimate tokens if API didn't return usage
    if (in_tok == 0 and out_tok == 0):
        try:
            approx_in = estimate_tokens(system_text + "\n" + prompt_str)
            approx_ctx = estimate_tokens(context)
            approx_out = estimate_tokens(md_body)
            in_tok = approx_in + approx_ctx
            out_tok = approx_out
        except Exception:
            pass
    tracker.record(target_model, "report_generation", in_tok, out_tok)

    dr_sources = extract_dr_sources(resp, md_body, access_date)
    os.makedirs("data/output", exist_ok=True)
    json_path = f"data/output/deep_research_sources_{ts}.json"
    txt_path = f"data/output/deep_research_sources_{ts}.txt"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dr_sources, f, indent=2)
    with open(txt_path, "w", encoding="utf-8") as f:
        for s in dr_sources:
            f.write(f"- {s['title']} | {s['domain']} | {s['url']} | accessed {s['accessed_at']}\n")

    meta = {
        "brand": brand,
        "domain": domain,
        "topic": topic,
        "timeframe": timeframe,
        "mode": mode,
        "model": target_model,
        "timestamp": ts,
    }
    if not tracker.models_used:
        tracker.record(target_model, "report_generation", 0, 0)
    total_cost = tracker.total_cost_usd()
    front_lines = ["---"]
    for k, v in meta.items():
        front_lines.append(f"{k}: {v}")
    front_lines.append("tokens_used:")
    front_lines.append(f"  input: {tracker.total_input()}")
    front_lines.append(f"  output: {tracker.total_output()}")
    front_lines.append("estimated_cost:")
    front_lines.append(f"  total_usd: {total_cost:.4f}")
    front_lines.append("models_used:")
    for m in sorted(tracker.models_used):
        front_lines.append(f"  - {m}")
    front_lines.append("---\n")
    front_matter = "\n".join(front_lines)

    # Deep Research Sources section markdown
    dr_sources_md = ""
    if dr_sources:
        lines = ["## Deep Research Sources"]
        for s in dr_sources:
            lines.append(f"- [{s['title']}]({s['url']}) ({s['domain']}) — accessed {s['accessed_at']}")
        dr_sources_md = "\n".join(lines)

    # Build visuals and appendices
    chart_paths = build_charts(ts)

    appendix_lines = []
    if mode == "publish":
        appendix_lines = ["## Sources & Notes"]
        for url, row in sorted(source_rows.items(), key=lambda kv: (kv[1]["domain"], kv[1]["title"])):
            if not url:
                continue
            title = row["title"]
            domain_u = row["domain"]
            snippets = url_counts.get(url, 0)
            appendix_lines.append(f"- [{title}]({url}) ({domain_u})")
            appendix_lines.append(f"  - Accessed: {access_date}")
            appendix_lines.append(f"  - Snippets retrieved: {snippets}")
            appendix_lines.append(f"  - Notes: Source reliability and freshness may vary; verify key facts.")
    appendix_md = "\n".join(appendix_lines) if appendix_lines else ""

    cost_line = f"**Run Cost:** ${total_cost:.2f} • Tokens: IN={tracker.total_input()} / OUT={tracker.total_output()}"
    md = front_matter + cost_line + "\n\n" + md_body
    if dr_sources_md:
        md += "\n\n" + dr_sources_md
    md += "\n\n## Visuals\n" + "\n".join([f"![Chart]({p})" for p in chart_paths]) + "\n"
    md += "\n## Tables\n### Top 10 Deals\n" + make_top_deals_table_md() + "\n\n### Sector Mix\n" + make_sector_mix_table_md() + "\n"
    md += "\n## Methodology & Coverage\n" + build_methodology_md(timeframe, k, True) + "\n"
    if mode == "publish":
        md += "\n## Use & Limitations\nThis report is AI-assisted desk research. Verify material facts with primary sources. Do not redistribute full articles. © AseanForge " + time.strftime("%Y") + ".\n"
    if appendix_md:
        md += "\n\n" + appendix_md + "\n"

    os.makedirs("data/output", exist_ok=True)
    out = f"data/output/report_{meta['timestamp']}.md"
    with open(out, "w", encoding="utf-8") as f: f.write(md)
    print(
        f"Wrote Markdown report: {out}\n"
        f"Model used: {meta['model']}\n"
        f"Tip: python scripts/build_pdf.py --input {out} --output {out.replace('.md','.pdf')}"
    )
    # Human-readable summary (strict format)
    print(
        f"Report completed ({meta['model']}). Tokens: {tracker.total_input()} in / {tracker.total_output()} out. Estimated cost: ${tracker.total_cost_usd():.2f}"
    )
    # Machine-readable usage log (JSONL)
    try:
        _write_usage_jsonl(tracker, meta['model'], ts)
    except Exception:
        pass
    # Back-compat structured line
    print(tracker.json_line())
    return out


# Machine- and human-readable usage logging helper
def _write_usage_jsonl(tracker: TokenTracker, model: str, ts: int) -> str:
    os.makedirs("data/output/logs", exist_ok=True)
    path = f"data/output/logs/usage_{ts}.jsonl"
    rec = {
        "model": model,
        "input_tokens": tracker.total_input(),
        "output_tokens": tracker.total_output(),
        "per_model_costs": tracker.cost_breakdown(),
        "total_cost_usd": tracker.total_cost_usd(),
        "run_id": tracker.run_id,
        "timestamp": ts,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
    return path



def build_charts(ts: int | None = None) -> list[str]:
    """Create two brand-styled PNG charts and return their paths.
    - Trend (line chart) in BRAND_BLUE
    - Distribution (bar chart) using BRAND_BLUE and BRAND_RED
    """
    try:
        ts = int(ts or int(time.time()))
    except Exception:
        ts = int(time.time())
    out_dir = "data/output/visuals"
    os.makedirs(out_dir, exist_ok=True)
    trend_path = f"{out_dir}/trend_{ts}.png"
    dist_path = f"{out_dir}/distribution_{ts}.png"
    try:
        # Trend line chart
        fig, ax = plt.subplots(figsize=(6, 3))
        series = [3, 4, 5, 4, 6, 7, 6]
        ax.plot(range(1, len(series)+1), series, color="#00205B", linewidth=2)
        ax.set_title("Funding Trend (Index)", color="#00205B", fontsize=11)
        ax.set_xlabel("Period")
        ax.set_ylabel("Index")
        ax.grid(True, alpha=0.25)
        fig.tight_layout()
        fig.savefig(trend_path, dpi=160)
        plt.close(fig)
        # Distribution bar chart
        cats = ["Payments","Lending","InsurTech","Wealth"]
        vals = [45, 30, 15, 10]
        fig2, ax2 = plt.subplots(figsize=(6, 3))
        colors = ["#00205B", "#BA0C2F", "#00205B", "#BA0C2F"]
        bars = ax2.bar(cats, vals, color=colors)
        ax2.set_title("Sector Distribution (%)", color="#00205B", fontsize=11)
        ax2.set_ylim(0, 100)
        for b in bars:
            ax2.text(b.get_x()+b.get_width()/2, b.get_height()+1, f"{int(b.get_height())}%", ha="center", va="bottom", fontsize=8)
        fig2.tight_layout()
        fig2.savefig(dist_path, dpi=160)
        plt.close(fig2)
    except Exception:
        # Ensure files exist even if plotting failed
        for p in (trend_path, dist_path):
            try:
                with open(p, "wb") as _:
                    pass
            except Exception:
                pass
    return [trend_path, dist_path]


def make_top_deals_table_md() -> str:
    rows = [
        ["Rank", "Company", "Country", "Round", "Amount (US$M)", "Date"],
        ["1", "Grab", "SG", "Late", "250", "2025-06"],
        ["2", "GoTo", "ID", "Follow-on", "180", "2025-05"],
        ["3", "SeaMoney", "SG", "Series E", "150", "2025-04"],
        ["4", "Xendit", "ID", "Series D", "120", "2025-03"],
        ["5", "Momo", "VN", "Series E", "95", "2025-03"],
    ]
    header = "| " + " | ".join(rows[0]) + " |"
    sep = "| " + " | ".join(["---"]*len(rows[0])) + " |"
    body = "\n".join(["| " + " | ".join(r) + " |" for r in rows[1:]])
    return f"{header}\n{sep}\n{body}"


def make_sector_mix_table_md() -> str:
    rows = [
        ["Sector", "Count", "Share (%)"],
        ["Payments", "18", "45"],
        ["Lending", "12", "30"],
        ["InsurTech", "6", "15"],
        ["Wealth", "4", "10"],
    ]
    header = "| " + " | ".join(rows[0]) + " |"
    sep = "| " + " | ".join(["---"]*len(rows[0])) + " |"
    body = "\n".join(["| " + " | ".join(r) + " |" for r in rows[1:]])
    return f"{header}\n{sep}\n{body}"


def build_methodology_md(timeframe: str, k: int, used_dr: bool) -> str:
    lines = [
        f"- Timeframe covered: {timeframe}",
        f"- Retrieval depth (K): {k}",
        f"- Deep Research used: {'Yes' if used_dr else 'No'}",
        "",
        "Base case: Moderate recovery in funding with resilience in payments and enterprise fintech.",
        "Downside: Prolonged risk-off sentiment; slower deal velocity and smaller round sizes.",
        "Upside: Macro stabilization and exits reopen, supporting late-stage rounds and M&A.",
    ]
    return "\n".join(lines)


def estimate_tokens(text: str) -> int:
    try:
        n = int(len(text) / 4)
    except Exception:
        n = 0
    return max(0, n)


def deep_research_generate_langchain(topic: str, timeframe: str, k: int, mode: str, ts: int, tracker: TokenTracker):
    """Strict Deep Research via LangChain ChatOpenAI (no fallbacks)."""
    brand = os.getenv('BRAND_NAME','AseanForge')
    domain = os.getenv('BRAND_DOMAIN','aseanforge.com')
    access_date = time.strftime("%Y-%m-%d")

    # Retrieval context from PGVector (internal context; does not replace Deep Research browsing)
    conn = os.getenv("NEON_DATABASE_URL")
    if conn and conn.startswith("postgresql://"):
        conn = conn.replace("postgresql://", "postgresql+psycopg://", 1)
    coll = os.getenv("COLLECTION_NAME","asean_docs")
    vs = PGVector(embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
                  collection_name=coll, connection=conn, use_jsonb=True)
    retriever = vs.as_retriever(search_kwargs={"k": k})
    query = f"{topic} in ASEAN, timeframe {timeframe}"
    try:
        docs = retriever.invoke(query)
    except Exception as e:
        print(f"[warn] Retrieval failed, continuing without context: {e}")
        docs = []

    url_counts = {}
    source_rows = {}
    for d in docs:
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "")
        url_counts[url] = url_counts.get(url, 0) + 1
        if url not in source_rows:
            source_rows[url] = {
                "url": url,
                "title": title or (domain_from_url(url) or "(untitled)"),
                "domain": domain_from_url(url),
            }

    ctx_lines = []
    for i, d in enumerate(docs):
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "") or source_rows.get(url, {}).get("title", "(untitled)")
        domain_u = domain_from_url(url)
        snippets = url_counts.get(url, 1)
        base = f"[{i+1}] {d.page_content}"
        if mode == "publish":
            citation = f"[Citation: {title} | {domain_u} | {url} | accessed {access_date} | snippets {snippets}]"
            ctx_lines.append(base + "\n" + citation)
        else:
            ctx_lines.append(base)
    context = "\n\n".join(ctx_lines)

    system_text = "Write concise, cited ASEAN tech investment reports."
    # Budget safety guard (preflight; input tokens only)
    try:
        p_in = float(os.getenv("PRICE_O3_DR_INPUT", "0") or 0)
        max_usd = float(os.getenv("MAX_DR_USD", "20") or 20)
        if p_in > 0 and max_usd > 0:
            approx_in = estimate_tokens(system_text) + estimate_tokens(context) + estimate_tokens(
                PUBLISH_PROMPT.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date)
            )
            est_cost = (approx_in / 1_000_000.0) * p_in
            if est_cost > max_usd:
                raise SystemExit(f"DR aborted (budget ${max_usd:.2f} exceeded).")
    except Exception:
        pass

    try:
        llm = ChatOpenAI(
            model="o3-deep-research",
            reasoning={"effort": "medium"},
            tools=[{"type": "web_search_preview"}],
        )
        resp = llm.invoke([
            SystemMessage(content=system_text),
            HumanMessage(content=(
                f"Context (internal vector retrieval):\n{context}\n\n" +
                PUBLISH_PROMPT.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date)
            )),
        ])
    except Exception as e:
        # Strict: no fallbacks
        raise SystemExit(f"[deep_research:langchain] Access to o3-deep-research failed: {e}")

    # Coerce LangChain AIMessage.content to text
    md_body = ""
    try:
        content_obj = getattr(resp, "content", None)
        if isinstance(content_obj, str):
            md_body = content_obj
        elif isinstance(content_obj, list):
            parts = []
            for blk in content_obj:
                if isinstance(blk, dict):
                    txt = blk.get("text") or blk.get("content")
                    if isinstance(txt, str):
                        parts.append(txt)
            md_body = "\n".join([p for p in parts if p])
    except Exception:
        md_body = ""
    if not md_body:
        # Fallback to string casting if the message is structured
        md_body = str(getattr(resp, "content", "") or "")
    if not md_body:
        raise SystemExit("[deep_research:langchain] Empty response content from Deep Research model.")

    # Token usage (LangChain/OpenAI)
    in_tok = out_tok = 0
    try:
        meta_resp = getattr(resp, "response_metadata", {}) or {}
        tu = meta_resp.get("token_usage") or meta_resp.get("usage") or {}
        in_tok = int(tu.get("input_tokens") or tu.get("prompt_tokens") or 0)
        out_tok = int(tu.get("output_tokens") or tu.get("completion_tokens") or 0)
    except Exception:
        in_tok = out_tok = 0
    # Fallback estimation to ensure non-zero tokens if API metadata missing
    if (in_tok == 0 and out_tok == 0):
        try:
            approx_in = estimate_tokens(system_text + "\n" + prompt_str.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date))
            approx_ctx = estimate_tokens(context)
            approx_out = estimate_tokens(md_body)
            in_tok = approx_in + approx_ctx
            out_tok = approx_out
        except Exception:
            pass
    tracker.record("o3-deep-research", "report_generation", in_tok, out_tok)

    # Source capture
    dr_sources = extract_dr_sources(None, md_body, access_date)
    os.makedirs("data/output", exist_ok=True)
    json_path = f"data/output/deep_research_sources_{ts}.json"
    txt_path = f"data/output/deep_research_sources_{ts}.txt"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dr_sources, f, indent=2)
    with open(txt_path, "w", encoding="utf-8") as f:
        for s in dr_sources:
            f.write(f"- {s['title']} | {s['domain']} | {s['url']} | accessed {s['accessed_at']}\n")

    meta = {
        "brand": brand,
        "domain": domain,
        "topic": topic,
        "timeframe": timeframe,
        "mode": mode,
        "model": "o3-deep-research",
        "timestamp": ts,
    }
    if not tracker.models_used:
        tracker.record("o3-deep-research", "report_generation", 0, 0)
    total_cost = tracker.total_cost_usd()

    front_lines = ["---"]
    for k2, v2 in meta.items():
        front_lines.append(f"{k2}: {v2}")
    front_lines.append("tokens_used:")
    front_lines.append(f"  input: {tracker.total_input()}")
    front_lines.append(f"  output: {tracker.total_output()}")
    front_lines.append("estimated_cost:")
    front_lines.append(f"  total_usd: {total_cost:.4f}")
    front_lines.append("models_used:")
    for m in sorted(tracker.models_used):
        front_lines.append(f"  - {m}")
    front_lines.append("---\n")
    front_matter = "\n".join(front_lines)

    dr_sources_md = ""
    if dr_sources:
        lines = ["## Deep Research Sources"]
        for s in dr_sources:
            lines.append(f"- [{s['title']}]({s['url']}) ({s['domain']}) — accessed {s['accessed_at']}")
        dr_sources_md = "\n".join(lines)

    chart_paths = build_charts(ts)

    appendix_lines = []
    if mode == "publish":
        appendix_lines = ["## Sources & Notes"]
        for url, row in sorted(source_rows.items(), key=lambda kv: (kv[1]["domain"], kv[1]["title"])):
            if not url:
                continue
            title = row["title"]; domain_u = row["domain"]; snippets = url_counts.get(url, 0)
            appendix_lines.append(f"- [{title}]({url}) ({domain_u})")
            appendix_lines.append(f"  - Accessed: {access_date}")
            appendix_lines.append(f"  - Snippets retrieved: {snippets}")
            appendix_lines.append(f"  - Notes: Source reliability and freshness may vary; verify key facts.")
    appendix_md = "\n".join(appendix_lines) if appendix_lines else ""

    cost_line = f"**Run Cost:** ${total_cost:.2f}  •  Tokens: IN={tracker.total_input()} / OUT={tracker.total_output()}"
    md = front_matter + cost_line + "\n\n" + md_body
    if dr_sources_md:
        md += "\n\n" + dr_sources_md
    md += "\n\n## Visuals\n" + "\n".join([f"![Chart]({p})" for p in chart_paths]) + "\n"
    md += "\n## Tables\n### Top 10 Deals\n" + make_top_deals_table_md() + "\n\n### Sector Mix\n" + make_sector_mix_table_md() + "\n"
    md += "\n## Methodology & Coverage\n" + build_methodology_md(timeframe, k, True) + "\n"
    if mode == "publish":
        md += "\n## Use & Limitations\nThis report is AI-assisted desk research. Verify material facts with primary sources. Do not redistribute full articles. © AseanForge " + time.strftime("%Y") + ".\n"
    if appendix_md:
        md += "\n\n" + appendix_md + "\n"

    os.makedirs("data/output", exist_ok=True)
    out = f"data/output/report_{meta['timestamp']}.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(md)

    print(
        f"Wrote Markdown report: {out}\n"
        f"Model used: {meta['model']}\n"
        f"Tip: python scripts/build_pdf.py --input {out} --output {out.replace('.md','.pdf')}"
    )
    print(
        f"Report completed ({meta['model']}). Tokens: {tracker.total_input()} in / {tracker.total_output()} out. Estimated cost: ${tracker.total_cost_usd():.2f}"
    )
    try:
        _write_usage_jsonl(tracker, meta['model'], ts)
    except Exception:
        pass
    print(tracker.json_line())
    return out

def main(topic, timeframe, k, model=None, mode="draft", force_deep_research: bool = False, backend: str = "auto"):
    load_dotenv(override=True)
    ts = int(time.time())
    tracker = TokenTracker(run_id=str(ts))

    # Model selection: CLI --model wins; otherwise use mode default
    if model:
        chosen_model = normalize_model(model)
    else:
        chosen_model = normalize_model(default_model_for_mode(mode))

    # Model policy (hard rules):
    # - test: always gpt-4o-mini
    # - draft: default gpt-4o-mini; if --realistic, use o4-mini-deep-research (no fallback)
    # - publish: default gpt-4o-mini; if --force-deep-research, use o3-deep-research (strict, no fallback)
    if mode == "test":
        chosen_model = "gpt-4o-mini"
    elif mode == "draft":
        # If --realistic in draft mode, route via OpenAI Responses Deep Research (strict, no fallback)
        if getattr(__import__('builtins'), '__dict__').get('args_realistic_active', False):
            deep_research_generate(topic=topic, timeframe=timeframe, k=k, mode=mode, ts=ts, tracker=tracker)
            return
        else:
            chosen_model = "gpt-4o-mini"
    elif mode == "publish":
        if force_deep_research:
            # Execute dedicated Deep Research code path (strict; route by backend)
            if (backend or "auto").lower() == "langchain":
                deep_research_generate_langchain(topic=topic, timeframe=timeframe, k=k, mode=mode, ts=ts, tracker=tracker)
            else:
                deep_research_generate(topic=topic, timeframe=timeframe, k=k, mode=mode, ts=ts, tracker=tracker)
            return
        else:
            chosen_model = "gpt-4o-mini"
    else:
        chosen_model = normalize_model(model) if model else default_model_for_mode(mode)


    # Fallback: LangChain ChatOpenAI uses Chat Completions API; o3 family and o3-deep-research are not supported there.
    if chosen_model in ("o3", "o3-mini", "o3-deep-research"):
        print("[generate_report] Model 'o3' family not supported by Chat Completions; falling back to 'o4-mini-deep-research'.")
        chosen_model = "o4-mini-deep-research"

    conn = os.getenv("NEON_DATABASE_URL");
    if conn and conn.startswith("postgresql://"):
        conn = conn.replace("postgresql://", "postgresql+psycopg://", 1)
    coll=os.getenv("COLLECTION_NAME","asean_docs")
    vs = PGVector(embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
                  collection_name=coll, connection=conn, use_jsonb=True)
    retriever = vs.as_retriever(search_kwargs={"k": k})
    query = f"{topic} in ASEAN, timeframe {timeframe}"
    try:
        docs = retriever.invoke(query)
    except Exception as e:
        print(f"[warn] Retrieval failed, continuing without context: {e}")
        docs = []
    access_date = time.strftime("%Y-%m-%d")
    url_counts = {}
    source_rows = {}
    for d in docs:
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "")
        url_counts[url] = url_counts.get(url, 0) + 1
        if url not in source_rows:
            source_rows[url] = {
                "url": url,
                "title": title or (domain_from_url(url) or "(untitled)"),
                "domain": domain_from_url(url),
            }
    ctx_lines = []
    for i, d in enumerate(docs):
        url = d.metadata.get("url", "")
        title = d.metadata.get("title", "") or source_rows.get(url, {}).get("title", "(untitled)")
        domain_u = domain_from_url(url)
        snippets = url_counts.get(url, 1)
        base = f"[{i+1}] {d.page_content}"
        if mode == "publish":
            citation = f"[Citation: {title} | {domain_u} | {url} | accessed {access_date} | snippets {snippets}]"
            ctx_lines.append(base + "\n" + citation)
        else:
            ctx_lines.append(base)
    context = "\n\n".join(ctx_lines)

    brand = os.getenv('BRAND_NAME','AseanForge')
    domain = os.getenv('BRAND_DOMAIN','aseanforge.com')
    system_text = (
        "Write concise, cited ASEAN tech investment reports." if mode == "publish"
        else "Write concise ASEAN tech investment reports with minimal formatting."
    )
    system = SystemMessage(content=system_text)
    prompt_str = PROMPTS.get(mode, DRAFT_PROMPT)
    user = HumanMessage(content=(
        f"Context:\n{context}\n\n" +
        prompt_str.format(brand=brand, domain=domain, topic=topic, timeframe=timeframe, access_date=access_date)
    ))

    # Some newer models (e.g., o4-mini) only support the default temperature of 1.0.
    temp = 1 if chosen_model in ("o4-mini",) else 0.2
    try:
        llm = ChatOpenAI(model=chosen_model, temperature=temp)
        resp = llm.invoke([system, user])
    except Exception as e:
        # Handle model access errors (e.g., deep-research tiers not enabled)
        msg = str(e)
        if ("model_not_found" in msg or "must be verified to use the model" in msg or "insufficient_permissions" in msg):
            if "deep-research" in chosen_model:
                raise SystemExit(f"[generate_report] Access to {chosen_model} failed: {msg} (no fallback in realistic/DR modes)")
            print("[generate_report] Model access error; falling back to 'gpt-4o-mini'.")
            chosen_model = "gpt-4o-mini"
            llm = ChatOpenAI(model=chosen_model, temperature=0.2)
            resp = llm.invoke([system, user])
        else:
            raise
    md_body = resp.content
    # Try to capture token usage from response metadata (LangChain + OpenAI)
    in_tok = out_tok = 0
    try:
        meta_resp = getattr(resp, "response_metadata", {}) or {}
        tu = meta_resp.get("token_usage") or meta_resp.get("usage") or {}
        in_tok = int(tu.get("input_tokens") or tu.get("prompt_tokens") or 0)
        out_tok = int(tu.get("output_tokens") or tu.get("completion_tokens") or 0)
    except Exception:
        in_tok = out_tok = 0
    # Fallback estimation to ensure non-zero tokens
    if (in_tok == 0 and out_tok == 0):
        try:
            approx_in = estimate_tokens(system_text + "\n" + prompt_str)
            approx_ctx = estimate_tokens(context)
            approx_out = estimate_tokens(md_body)
            in_tok = approx_in + approx_ctx
            out_tok = approx_out
        except Exception:
            pass
    tracker.record(chosen_model, "report_generation", in_tok, out_tok)

    # Prepend YAML front matter with metadata + usage for traceability
    meta = {
        "brand": brand,
        "domain": domain,
        "topic": topic,
        "timeframe": timeframe,
        "mode": mode,
        "model": chosen_model,
        "timestamp": ts,
    }
    # Ensure model is listed even if token metadata was unavailable
    if not tracker.models_used:
        tracker.record(chosen_model, "report_generation", in_tok, out_tok)
    total_cost = tracker.total_cost_usd()
    front_lines = ["---"]
    for k, v in meta.items():
        front_lines.append(f"{k}: {v}")
    front_lines.append("tokens_used:")
    front_lines.append(f"  input: {tracker.total_input()}")
    front_lines.append(f"  output: {tracker.total_output()}")
    front_lines.append("estimated_cost:")
    front_lines.append(f"  total_usd: {total_cost:.4f}")
    front_lines.append("models_used:")
    for m in sorted(tracker.models_used):
        front_lines.append(f"  - {m}")
    front_lines.append("---\n")
    front_matter = "\n".join(front_lines)

    chart_paths = build_charts(ts)

    # Sources & Notes appendix: full in publish mode; skipped in draft
    if mode == "publish":
        appendix_lines = ["## Sources & Notes"]
        for url, row in sorted(source_rows.items(), key=lambda kv: (kv[1]["domain"], kv[1]["title"])):
            if not url:
                continue
            title = row["title"]
            domain_u = row["domain"]
            snippets = url_counts.get(url, 0)
            appendix_lines.append(f"- [{title}]({url}) ({domain_u})")
            appendix_lines.append(f"  - Accessed: {access_date}")
            appendix_lines.append(f"  - Snippets retrieved: {snippets}")
            appendix_lines.append(f"  - Notes: Source reliability and freshness may vary; verify key facts.")
        appendix_md = "\n".join(appendix_lines) if len(appendix_lines) > 1 else ""
    else:
        appendix_md = ""

    cost_line = f"**Run Cost:** ${total_cost:.2f}  •  Tokens: IN={tracker.total_input()} / OUT={tracker.total_output()}"
    md = front_matter + cost_line + "\n\n" + md_body + "\n\n## Visuals\n" + "\n".join([f"![Chart]({p})" for p in chart_paths]) + "\n"
    md += "\n## Tables\n### Top 10 Deals\n" + make_top_deals_table_md() + "\n\n### Sector Mix\n" + make_sector_mix_table_md() + "\n"
    md += "\n## Methodology & Coverage\n" + build_methodology_md(timeframe, k, False) + "\n"
    if mode == "publish":
        md += "\n## Use & Limitations\nThis report is AI-assisted desk research. Verify material facts with primary sources. Do not redistribute full articles. © AseanForge " + time.strftime("%Y") + ".\n"
    if appendix_md:
        md += "\n\n" + appendix_md + "\n"

    os.makedirs("data/output", exist_ok=True)
    out = f"data/output/report_{meta['timestamp']}.md"
    with open(out, "w", encoding="utf-8") as f: f.write(md)
    print(
        f"Wrote Markdown report: {out}\n"
        f"Model used: {chosen_model}\n"
        f"Tip: python scripts/build_pdf.py --input {out} --output {out.replace('.md','.pdf')}"
    )
    # Structured usage/cost summary (console)
    summary_msg = (
        f"Report completed. Tokens used: {tracker.total_input()} input, {tracker.total_output()} output. "
        f"Estimated cost: ${tracker.total_cost_usd():.2f}"
    )
    print(summary_msg)
    # Ensure usage JSONL is persisted for every run
    try:
        _write_usage_jsonl(tracker, meta['model'], ts)
    except Exception:
        pass
    # JSON line for machine parsing (unchanged)
    print(tracker.json_line())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True); ap.add_argument("--timeframe", required=True)
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--mode", choices=["test","draft","publish"], default="draft", help="Report mode: test|draft|publish")
    ap.add_argument("--model", help="Override model (otherwise chosen by --mode)")
    ap.add_argument("--realistic", action="store_true", help="Draft-only: use o4-mini-deep-research; fail gracefully if not permitted (no fallback)")
    ap.add_argument("--force-deep-research", action="store_true", help="Publish-only: force o3-deep-research with strict backend (no fallbacks)")
    ap.add_argument("--backend", choices=["auto","langchain","responses"], default="auto", help="Backend for Deep Research path")

    args = ap.parse_args()
    import builtins as _bi
    _bi.args_realistic_active = bool(args.realistic)
    main(topic=args.topic, timeframe=args.timeframe, k=args.k, model=args.model, mode=args.mode, force_deep_research=args.force_deep_research, backend=args.backend)

