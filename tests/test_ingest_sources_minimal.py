import math
from scripts.ingest_sources import chunk_text, extract_metadata


def test_chunk_text_tokenish_bounds():
    # ~4 chars per token heuristic; target ~600 tokens (default)
    text = ("A" * 2400) + ("B" * 2400) + ("C" * 1200)  # ~ 2400+2400+1200 = 600+600+300 tokens
    parts = list(chunk_text(text))
    assert len(parts) >= 3
    # Each chunk should be within ~500-700 tokens => ~2000-2800 chars
    for p in parts:
        assert 2000 <= len(p) <= 3000


def test_extract_metadata_common_fields():
    item = {
        "markdown": "# Title\nBody",
        "metadata": {
            "title": "Sample Title",
            "sourceURL": "https://example.com/post/123",
            "date": "2024-04-01",
        },
    }
    url, title, domain, published_at = extract_metadata(item)
    assert url == "https://example.com/post/123"
    assert title == "Sample Title"
    assert domain == "example.com"
    assert published_at.year == 2024 and published_at.month == 4 and published_at.day == 1

