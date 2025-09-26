import os, argparse, markdown, time
from dotenv import load_dotenv
# weasyprint imported lazily in main to allow fallback when system libs are missing

BRAND_BLUE = "#00205B"; BRAND_RED = "#BA0C2F"; BRAND_WHITE = "#FFFFFF"

HTML_TMPL = """<html><head><meta charset="utf-8"/>{style}</head><body>
{watermark}
<div class="header"><img src="{logo}" height="40"/><div class="title">{brand} — ASEAN Tech Investment Report</div></div>
<div class="small">{domain}</div>
{content}
</body></html>"""

def parse_front_matter(md: str) -> dict:
    meta = {}
    if md.startswith("---"):
        end = md.find("\n---", 3)
        if end != -1:
            block = md[3:end].strip().splitlines()
            for ln in block:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    meta[k.strip()] = v.strip()
    return meta


def slugify(text: str) -> str:
    import re as _re
    s = (text or "").lower()
    s = _re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "report"


def compute_version_id(meta: dict, fallback_topic: str) -> str:
    # Prefer explicit version_id in YAML
    ver = meta.get("version_id")
    if ver:
        return ver
    # Auto-generate: AF-<YYYYQ>-<slug>-001
    tm = time.localtime()
    year = tm.tm_year
    q = (tm.tm_mon - 1) // 3 + 1
    slug = slugify(meta.get("topic") or fallback_topic)
    return f"AF-{year}Q{q}-{slug}-001"

def resolve_mode(cli_mode: str | None, meta: dict) -> str:
    if cli_mode and cli_mode != "auto":
        return cli_mode
    return meta.get("mode", "publish")



def extract_first_h1(md: str) -> str | None:
    # Skip YAML front matter if present
    start = 0
    if md.startswith("---"):
        end = md.find("\n---", 3)
        if end != -1:
            start = end + 4
    for ln in md[start:].splitlines():
        if ln.startswith("# "):
            return ln[2:].strip()
    return None

def extract_cost_line(md: str) -> str | None:
    # Look for the visible cost block added right after front matter
    start = 0
    if md.startswith("---"):
        end = md.find("\n---", 3)
        if end != -1:
            start = end + 4
    for ln in md[start:].splitlines():
        if ln.strip().startswith("**Run Cost:**"):
            return ln.strip().strip()
    return None


def main(input, output, logo, mode="auto"):
    load_dotenv(override=True)
    brand = os.getenv("BRAND_NAME","AseanForge"); domain=os.getenv("BRAND_DOMAIN","aseanforge.com")
    with open(input, "r", encoding="utf-8") as f: md = f.read()
    meta = parse_front_matter(md)
    mode = resolve_mode(mode, meta)
    date_str = time.strftime("%Y-%m-%d")
    version_id = compute_version_id(meta, meta.get("topic", ""))
    cost_line_text = extract_cost_line(md)

    # Build minimal CSS and watermark for WeasyPrint path
    style = ""
    watermark_html = ""
    if mode == "publish":
        style = f"""
        <style>
        @page {{ size: A4; margin: 20mm;
            @bottom-left {{ content: '{brand} | {date_str}'; color: {BRAND_BLUE}; font-size: 9pt; }}
            @bottom-right {{ content: 'Page ' counter(page) ' | v{version_id}'; color: {BRAND_BLUE}; font-size: 9pt; }}
            @bottom-center {{ content: 'support@aseanforge.com'; color: {BRAND_BLUE}; font-size: 8pt; }}
        }}
        body {{ font-family: sans-serif; }}
        .header .title {{ color: {BRAND_BLUE}; font-weight: 700; }}
        .small {{ color: {BRAND_BLUE}; }}
        </style>
        """
    else:
        style = f"""
        <style>
        @page {{ size: A4; margin: 20mm;
            @bottom-left {{ content: '{brand} | {date_str}'; color: {BRAND_BLUE}; font-size: 9pt; }}
        }}
        body {{ font-family: sans-serif; }}
        .header .title {{ color: {BRAND_BLUE}; font-weight: 700; }}
        .small {{ color: {BRAND_BLUE}; }}
        .watermark {{ position: fixed; top: 12px; right: 12px; color: {BRAND_RED}; opacity: 0.25; font-weight: 700; }}
        </style>
        """
        watermark_html = "<div class='watermark'>DRAFT</div>"

    html_body = markdown.markdown(md, extensions=["tables","fenced_code"])
    html = HTML_TMPL.format(style=style, watermark=watermark_html, logo=logo, brand=brand, domain=domain, content=html_body)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    # Try WeasyPrint first; if it fails (system libs), fall back to ReportLab
    try:
        from weasyprint import HTML
        HTML(string=html, base_url=".").write_pdf(output)
        print(f"Wrote PDF (WeasyPrint, mode={mode}): {output}")
        return
    except Exception as we:
        print("[warn] WeasyPrint unavailable, using ReportLab fallback:", we)
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import Table, TableStyle

        def draw_header(c):
            W, H = A4
            y = H - 2*cm
            # Logo
            try:
                if os.path.exists(logo):
                    c.drawImage(ImageReader(logo), 2*cm, y-1.2*cm, width=3.5*cm, height=1.2*cm, mask='auto')
            except Exception:
                pass
            # Brand header
            c.setFont("Helvetica-Bold", 14)
            c.setFillColor(colors.HexColor(BRAND_BLUE))
            c.drawString(6*cm, y-0.5*cm, f"{brand} — ASEAN Tech Investment Report")
            c.setFont("Helvetica", 9)
            c.drawString(6*cm, y-1.0*cm, domain)
            # Draft watermark (subtle, corner)
            if mode == "draft":
                c.setFont("Helvetica-Bold", 10)
                c.setFillColor(colors.HexColor(BRAND_RED))
                c.drawRightString(W - 2*cm, y-0.5*cm, "DRAFT")
            return y - 2.2*cm

        def draw_footer(c, page_offset: int = 0, include_page_num: bool = True):
            W, _ = A4
            c.setFont("Helvetica", 9)
            c.setFillColor(colors.HexColor(BRAND_BLUE))
            # Left footer: brand + date
            c.drawString(2*cm, 1.5*cm, f"{brand} | {date_str}")
            # Second line: support contact
            c.setFont("Helvetica", 8)
            c.drawString(2*cm, 1.2*cm, "support@aseanforge.com")
            # Right footer: page number and version in publish mode; start numbering from content pages
            if mode == "publish" and include_page_num:
                c.setFont("Helvetica", 9)
                displayed = c.getPageNumber() - page_offset
                if displayed >= 1:
                    c.drawRightString(W-2*cm, 1.5*cm, f"Page {displayed} | v{version_id}")

        # Render
        c = canvas.Canvas(output, pagesize=A4)
        W, H = A4

        # --- Cover page ---
        def draw_cover(c, title_text: str):
            # Logo centered near top
            try:
                if os.path.exists(logo):
                    logo_w = 6*cm; logo_h = 2*cm
                    c.drawImage(ImageReader(logo), (W - logo_w)/2, H - 5*cm, width=logo_w, height=logo_h, mask='auto')
            except Exception:
                pass
            # Title and date in brand colors
            c.setFillColor(colors.HexColor(BRAND_BLUE))
            c.setFont("Helvetica-Bold", 24)
            c.drawCentredString(W/2, H/2 + 2*cm, title_text)
            c.setFont("Helvetica", 12)
            c.drawCentredString(W/2, H/2 + 1*cm, f"{date_str}")
            # Cost line under date, if available
            if cost_line_text:
                c.setFont("Helvetica", 10)
                c.setFillColor(colors.HexColor(BRAND_BLUE))
                c.drawCentredString(W/2, H/2, cost_line_text)
            # Draft watermark visible on cover
            if mode == "draft":
                c.setFont("Helvetica-Bold", 14)
                c.setFillColor(colors.HexColor(BRAND_RED))
                c.drawRightString(W - 2*cm, H - 2*cm, "DRAFT")

        title_text = extract_first_h1(md) or "ASEAN Tech Investment Report"
        page_offset = 1  # start numbering from content pages
        draw_cover(c, title_text)
        # Footer on cover without page number
        draw_footer(c, page_offset=page_offset, include_page_num=False)
        c.showPage()

        # --- Content pages ---
        y = draw_header(c)
        left = 2*cm
        right = 2*cm
        content_w = W - left - right

        import re as _re
        IMG_RE = _re.compile(r"^!\[(?P<alt>.*?)\]\((?P<src>[^\)]+)\)\s*$")

        # Parse markdown into simple blocks (headings, images, paragraphs, tables)
        def parse_blocks(md_text: str):
            # skip YAML front matter
            start = 0
            if md_text.startswith("---"):
                end = md_text.find("\n---", 3)
                if end != -1:
                    start = end + 4
            lines = md_text[start:].splitlines()
            blocks = []
            i = 0
            para_buf = []
            def flush_para():
                nonlocal para_buf
                if para_buf:
                    blocks.append({"type": "para", "text": " ".join(para_buf).strip()})
                    para_buf = []
            def is_table_sep(s: str) -> bool:
                s = s.strip()
                return s.startswith("|") and s.endswith("|") and set(s.replace("|"," ").replace(" ", "").replace(":","-")).issubset(set("-"))
            while i < len(lines):
                ln = lines[i]
                m = IMG_RE.match(ln.strip())
                if m:
                    flush_para()
                    blocks.append({"type": "image", "src": m.group("src"), "alt": m.group("alt")})
                    i += 1
                    continue
                if ln.startswith("# "):
                    flush_para(); blocks.append({"type": "h1", "text": ln[2:].strip()}); i += 1; continue
                if ln.startswith("## "):
                    flush_para(); blocks.append({"type": "h2", "text": ln[3:].strip()}); i += 1; continue
                if ln.startswith("### "):
                    flush_para(); blocks.append({"type": "h3", "text": ln[4:].strip()}); i += 1; continue
                # Detect Markdown table: header row, separator, then data rows
                if "|" in ln and i + 1 < len(lines) and is_table_sep(lines[i+1]):
                    flush_para()
                    # collect header, separator, then rows until blank or non-table
                    header_line = ln.strip()
                    sep_line = lines[i+1].strip()
                    i += 2
                    data_rows = []
                    while i < len(lines) and "|" in lines[i] and lines[i].strip():
                        data_rows.append(lines[i].strip())
                        i += 1
                    def split_row(row: str):
                        cells = [c.strip() for c in row.strip("|").split("|")]
                        return cells
                    header_cells = split_row(header_line)
                    rows = [header_cells] + [split_row(r) for r in data_rows]
                    blocks.append({"type": "table", "rows": rows})
                    continue
                if not ln.strip():
                    flush_para(); i += 1; continue
                para_buf.append(ln.strip()); i += 1
            flush_para()
            return blocks

        # Word wrap utility using string width
        from reportlab.pdfbase import pdfmetrics
        def wrap_lines(text: str, font_name: str, font_size: int, max_width: float):
            words = text.split()
            line = ""
            for w in words:
                candidate = (line + (" " if line else "") + w)
                if pdfmetrics.stringWidth(candidate, font_name, font_size) <= max_width:
                    line = candidate
                else:
                    if line:
                        yield line
                        line = w
                    else:
                        # very long single word; hard break
                        yield w
                        line = ""
            if line:
                yield line

        blocks = parse_blocks(md)
        for blk in blocks:
            if blk["type"] == "h1":
                c.setFillColor(colors.HexColor(BRAND_BLUE))
                c.setFont("Helvetica-Bold", 18)
                y -= 8
                c.drawString(left, y, blk["text"]) ; y -= 22
            elif blk["type"] == "h2":
                c.setFillColor(colors.black)
                c.setFont("Helvetica-Bold", 14)
                y -= 6
                c.drawString(left, y, blk["text"]) ; y -= 18
            elif blk["type"] == "h3":
                c.setFillColor(colors.black)
                c.setFont("Helvetica-Bold", 12)
                y -= 4
                c.drawString(left, y, blk["text"]) ; y -= 14
            elif blk["type"] == "image":
                try:
                    if os.path.exists(blk["src"]):
                        ir = ImageReader(blk["src"])
                        iw, ih = ir.getSize()
                        scale = min(content_w / float(iw), 1.0)
                        dw, dh = iw * scale, ih * scale
                        x = left + (content_w - dw) / 2
                        if y - dh < 2.5*cm:
                            draw_footer(c, page_offset=page_offset, include_page_num=True)
                            c.showPage(); y = draw_header(c)
                        c.drawImage(ir, x, y - dh, width=dw, height=dh, preserveAspectRatio=True, mask='auto')
                        y -= (dh + 10)
                    else:
                        # missing image placeholder text
                        c.setFillColor(colors.red)
                        c.setFont("Helvetica-Oblique", 10)
                        c.drawString(left, y, f"[Image not found: {blk['src']}]")
                        y -= 14
                except Exception:
                    pass
            elif blk["type"] == "table":
                try:
                    data = blk.get("rows", [])
                    if data:
                        # Compute column widths evenly across content width
                        ncols = max(1, len(data[0]))
                        col_w = content_w / ncols
                        tbl = Table(data, colWidths=[col_w]*ncols)
                        ts = TableStyle([
                            ("BACKGROUND", (0,0), (-1,0), colors.HexColor(BRAND_BLUE)),
                            ("TEXTCOLOR", (0,0), (-1,0), colors.white),
                            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                            ("FONTSIZE", (0,0), (-1,-1), 9),
                            ("ALIGN", (0,0), (-1,-1), "LEFT"),
                            ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor(BRAND_BLUE)),
                            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.HexColor("#F7F9FC")]),
                            ("LEFTPADDING", (0,0), (-1,-1), 6), ("RIGHTPADDING", (0,0), (-1,-1), 6),
                            ("TOPPADDING", (0,0), (-1,-1), 4), ("BOTTOMPADDING", (0,0), (-1,-1), 4),
                        ])
                        tbl.setStyle(ts)
                        w, h = tbl.wrap(content_w, y)
                        if y - h < 2.5*cm:
                            draw_footer(c, page_offset=page_offset, include_page_num=True)
                            c.showPage(); y = draw_header(c)
                        tbl.drawOn(c, left, y - h)
                        y -= (h + 12)
                except Exception:
                    pass
            else:  # paragraph
                c.setFillColor(colors.black)
                c.setFont("Times-Roman", 10)
                for ln in wrap_lines(blk.get("text", ""), "Times-Roman", 10, content_w):
                    if y < 2.5*cm:
                        draw_footer(c, page_offset=page_offset, include_page_num=True)
                        c.showPage(); y = draw_header(c)
                        c.setFont("Times-Roman", 10); c.setFillColor(colors.black)
                    c.drawString(left, y, ln); y -= 14
                y -= 6  # paragraph spacing

        draw_footer(c, page_offset=page_offset, include_page_num=True)
        c.showPage()
        c.save()
        print(f"Wrote PDF (ReportLab, mode={mode}): {output}")
    except Exception as rl:
        raise SystemExit(f"Failed to build PDF with both WeasyPrint and ReportLab fallback: {rl}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True); ap.add_argument("--output", required=True)
    ap.add_argument("--logo", default="assets/logo.png")
    ap.add_argument("--mode", choices=["auto","draft","publish"], default="auto", help="auto=read from YAML front matter; or override explicitly")
    main(**vars(ap.parse_args()))

