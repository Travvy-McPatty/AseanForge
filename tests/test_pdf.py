import sys
import types
from pathlib import Path

import pytest
from pypdf import PdfReader

from scripts.build_pdf import parse_front_matter, resolve_mode, main as build_pdf_main


def test_front_matter_and_mode_resolution():
    md = """---
mode: draft
region: SEA
---
# Title
Body
"""
    meta = parse_front_matter(md)
    assert meta["mode"] == "draft"
    assert meta["region"] == "SEA"

    # auto -> use front matter
    assert resolve_mode("auto", meta) == "draft"
    # explicit override wins
    assert resolve_mode("publish", meta) == "publish"
    # default when absent
    assert resolve_mode("auto", {}) == "publish"


def _force_reportlab(monkeypatch):
    class DummyHTML:
        def __init__(self, *a, **k):
            pass
        def write_pdf(self, *a, **k):
            raise RuntimeError("force ReportLab path in tests")
    fake = types.SimpleNamespace(HTML=DummyHTML)
    monkeypatch.setitem(sys.modules, "weasyprint", fake)


def _fix_date(monkeypatch):
    monkeypatch.setattr("time.strftime", lambda fmt: "2024-01-02")


@pytest.mark.parametrize("mode,expect_draft_text,expect_page_num", [
    ("draft", True, False),
    ("publish", False, True),
])
def test_pdf_modes_watermark_and_footer(tmp_path: Path, monkeypatch, mode, expect_draft_text, expect_page_num):
    _force_reportlab(monkeypatch)
    _fix_date(monkeypatch)

    md = f"""---
mode: {mode}
---
# Heading\nSome content line that will appear in the PDF.
"""
    md_path = tmp_path / f"input_{mode}.md"
    md_path.write_text(md, encoding="utf-8")

    out_path = tmp_path / f"out_{mode}.pdf"

    # Build PDF (forces ReportLab fallback)
    build_pdf_main(str(md_path), str(out_path), logo="assets/logo.png", mode="auto")

    assert out_path.exists() and out_path.stat().st_size > 0

    # Extract text and assert simple presence checks
    reader = PdfReader(str(out_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)

    # Brand and fixed footer date
    assert "AseanForge | 2024-01-02" in text

    # Draft watermark appears only in draft; page number only in publish
    assert ("DRAFT" in text) is expect_draft_text
    assert ("Page 1" in text) is expect_page_num

