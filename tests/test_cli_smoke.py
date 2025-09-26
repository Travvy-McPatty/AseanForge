import os
import sys
import subprocess
import re
from pathlib import Path

import pytest
from pypdf import PdfReader
from PIL import Image


@pytest.fixture()
def cli_env_and_stubs(tmp_path: Path):
    """Create stub modules and environment for CLI subprocess tests.
    - Stubs external services: firecrawl, langchain_postgres, langchain_openai, weasyprint
    - Ensures no network/Neon are used; forces ReportLab path for PDFs
    - Returns (env, stubs_dir)
    """
    stubs = tmp_path / "stubs"
    (stubs / "langchain_postgres").mkdir(parents=True, exist_ok=True)
    (stubs / "langchain_openai").mkdir(parents=True, exist_ok=True)

    # firecrawl stub
    (stubs / "firecrawl.py").write_text(
        """
class Firecrawl:
    def __init__(self, api_key=None):
        pass
    def scrape(self, url, formats=None):
        class Doc:
            def __init__(self):
                self.markdown = "# Stub Title\\n\\n" + ("Body " * 200)
                self.data = {"metadata": {"title": "Stubbed Title"}}
        return Doc()
""",
        encoding="utf-8",
    )

    # langchain_postgres stub
    (stubs / "langchain_postgres" / "__init__.py").write_text(
        """
class StubDoc:
    def __init__(self, text, metadata):
        self.page_content = text
        self.metadata = metadata

class _Retriever:
    def __init__(self, k=3):
        self.k = k
    def invoke(self, query):
        # Deterministic set of docs with repeated URL for snippet counting
        return [
            StubDoc("Snippet A", {"url": "https://example.com/a", "title": "Source A"}),
            StubDoc("Snippet A2", {"url": "https://example.com/a", "title": "Source A"}),
            StubDoc("Snippet B", {"url": "https://example.com/b", "title": "Source B"}),
        ][: self.k]

class PGVector:
    def __init__(self, embeddings=None, collection_name=None, connection=None, use_jsonb=None):
        pass
    def add_texts(self, texts, metadatas=None):
        return None
    def as_retriever(self, search_kwargs=None):
        k = (search_kwargs or {}).get("k", 3)
        return _Retriever(k=k)
""",
        encoding="utf-8",
    )

    # langchain_openai stub
    (stubs / "langchain_openai" / "__init__.py").write_text(
        """
class OpenAIEmbeddings:
    def __init__(self, model=None):
        pass

class _Resp:
    def __init__(self):
        self.content = "## Body\\nGenerated content."
        self.response_metadata = {"token_usage": {"input_tokens": 10, "output_tokens": 20}}

class ChatOpenAI:
    def __init__(self, model=None, temperature=None):
        self.model = model
    def invoke(self, messages):
        return _Resp()
""",
        encoding="utf-8",
    )

    # weasyprint stub to force ReportLab fallback
    (stubs / "weasyprint.py").write_text(
        """
class HTML:
    def __init__(self, string=None, base_url=None):
        pass
    def write_pdf(self, output):
        raise RuntimeError("force ReportLab fallback in tests")
""",
        encoding="utf-8",
    )

    env = os.environ.copy()
    # Prepend stubs path so our modules win over real ones
    env["PYTHONPATH"] = str(stubs) + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    # Ensure no Neon DB is used
    env.pop("NEON_DATABASE_URL", None)
    # Provide a fake API key so Firecrawl init doesn't fail (stub doesn't care)
    env["FIRECRAWL_API_KEY"] = "test-key"

    return env, stubs


def test_cli_scrape_ingest_no_track_metadata(tmp_path: Path, cli_env_and_stubs):
    env, _ = cli_env_and_stubs
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "scrape_ingest.py"

    # Run with minimal limit and no DB tracking; use tmp as cwd to isolate cache path
    res = subprocess.run(
        [sys.executable, str(script), "--limit", "1", "--no-track-metadata"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert res.returncode == 0, f"stderr: {res.stderr}\nstdout: {res.stdout}"

    # Verify cache file was created with non-zero size
    cache_path = tmp_path / "data" / "ingest_cache.json"
    assert cache_path.exists() and cache_path.stat().st_size > 0


def _pdf_text(path: Path) -> str:
    reader = PdfReader(str(path))
    return "\n".join((p.extract_text() or "") for p in reader.pages)


@pytest.mark.parametrize(
    "mode_arg, front_matter, expect_draft, expect_page_num",
    [
        ("draft", None, True, False),
        ("publish", None, False, True),
        ("auto", "mode: draft\n", True, False),
    ],
)
def test_cli_build_pdf_modes(tmp_path: Path, cli_env_and_stubs, mode_arg, front_matter, expect_draft, expect_page_num):
    env, _ = cli_env_and_stubs
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "build_pdf.py"

    # Create a small test image
    img_path = tmp_path / "img.png"
    Image.new("RGB", (120, 60), color=(200, 20, 20)).save(img_path)

    # Create markdown input with headings, paragraph, image, and a missing image reference
    content = "\n".join([
        "# Heading",
        "## Subheading",
        "### Tertiary",
        "",
        "This is a paragraph that should wrap across the page width for readability.",
        f"![Chart]({img_path})",
        "![Missing](nope.png)",
        "",
    ])
    if front_matter:
        md = f"---\n{front_matter}---\n{content}"
    else:
        md = content
    md_path = tmp_path / f"input_{mode_arg}.md"
    md_path.write_text(md, encoding="utf-8")

    out_path = tmp_path / f"out_{mode_arg}.pdf"

    args = [sys.executable, str(script), "--input", str(md_path), "--output", str(out_path), "--mode", mode_arg]
    res = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)
    assert res.returncode == 0, f"stderr: {res.stderr}\nstdout: {res.stdout}"

    assert out_path.exists() and out_path.stat().st_size > 0

    reader = PdfReader(str(out_path))
    assert len(reader.pages) >= 2  # cover + at least one content page

    cover_text = (reader.pages[0].extract_text() or "")
    content_text = "\n".join([(p.extract_text() or "") for p in reader.pages[1:]])

    # Cover page checks
    assert "Heading" in cover_text  # title from first H1


def _latest_report_md(root: Path) -> Path:
    out_dir = root / "data" / "output"
    candidates = list(out_dir.glob("report_*.md"))
    assert candidates, "No report markdown files found"
    return max(candidates, key=lambda p: p.stat().st_mtime)


def test_cli_generate_report_publish_sources_and_model(tmp_path: Path, cli_env_and_stubs):
    env, _ = cli_env_and_stubs
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "generate_report.py"

    args = [
        sys.executable,
        str(script),
        "--topic", "ASEAN fintech",
        "--timeframe", "2023-2024",
        "--k", "3",
        "--mode", "publish",
        "--model", "o3-mini",
    ]
    res = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)
    assert res.returncode == 0, f"stderr: {res.stderr}\nstdout: {res.stdout}"

    md_path = _latest_report_md(tmp_path)
    md = md_path.read_text(encoding="utf-8")

    # Front matter contains mode and chosen model
    assert "mode: publish" in md
    assert "model: o3-mini" in md

    # Appendix exists with our stubbed sources and snippet counts
    assert "## Sources & Notes" in md
    assert "[Source A](https://example.com/a)" in md
    assert "[Source B](https://example.com/b)" in md
    assert re.search(r"Accessed: \d{4}-\d{2}-\d{2}", md)
    assert "Snippets retrieved: 2" in md  # two entries for /a
    assert "Snippets retrieved: 1" in md  # one entry for /b


def test_cli_generate_report_draft_no_appendix(tmp_path: Path, cli_env_and_stubs):
    env, _ = cli_env_and_stubs
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "generate_report.py"

    args = [
        sys.executable,
        str(script),
        "--topic", "ASEAN AI",
        "--timeframe", "2024",
        "--k", "2",
        "--mode", "draft",
    ]
    res = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)
    assert res.returncode == 0, f"stderr: {res.stderr}\nstdout: {res.stdout}"

    md_path = _latest_report_md(tmp_path)
    md = md_path.read_text(encoding="utf-8")

    # Front matter contains draft mode and default draft model (gpt-4o-mini)
    assert "mode: draft" in md
    assert "model: gpt-4o-mini" in md

    # No appendix in draft mode
    assert "## Sources & Notes" not in md

    