## FEEDS: Discovery & Fetch Methods (MVP)

- Primary fetch policy: Firecrawl-first with HTTP fallback
- Rendering: Global render=true; selective render=false for fast HTML authorities (PDPC, SC, OJK, BI)
- Limits: discover_links=8, process_per_source=5, delay≈1200ms, redirects≤5
- Seeds are authoritative in configs/firecrawl_seed.json (mirrored in config/sources.yaml)
- Firecrawl v2 parameters used: formats=[markdown, html], pageOptions.waitFor(ms), timeout=60000, includeHtml=true, parsers=[pdf]; proxy: auto|stealth
- Stealth proxies enabled for ASEAN and OJK; others default to auto


### Authorities and Methods

Authority | Seed URL | Primary | Render Override
---|---|---|---
ASEAN | https://asean.org/category/news/ | Firecrawl | render=true
MAS (Media Releases) | https://www.mas.gov.sg/news/media-releases | Firecrawl | render=true
MAS (Speeches) | https://www.mas.gov.sg/news/speeches | Firecrawl | render=true
IMDA | https://www.imda.gov.sg/resources/press-releases-factsheets-and-speeches | Firecrawl | render=true
PDPC | https://www.pdpc.gov.sg/News-and-Events | HTTP/Firecrawl | render=false
BI | https://www.bi.go.id/id/publikasi/ruang-media/news-release/ | HTTP/Firecrawl | render=false
OJK | https://www.ojk.go.id/id/berita-dan-kegiatan/siaran-pers | HTTP/Firecrawl | render=false
KOMINFO | https://kominfo.go.id/siaran-pers | Firecrawl | render=true
BOT | https://www.bot.or.th/en/news-and-media/press-release | Firecrawl | render=true
BNM | https://www.bnm.gov.my/press-release | Firecrawl | render=true
SC | https://www.sc.com.my/resources/media/media-release | HTTP/Firecrawl | render=false
MCMC | https://www.mcmc.gov.my/media/media-releases | Firecrawl | render=true
BSP | https://www.bsp.gov.ph/SitePages/Media%20and%20Research/MediaReleases.aspx | Firecrawl | render=true
DICT | https://dict.gov.ph/category/press-releases/ | Firecrawl | render=true
MIC | https://english.mic.gov.vn/Pages/TinTuc/tintuckinhte.aspx | Firecrawl | render=true
SBV | https://www.sbv.gov.vn/webcenter/portal/en/home/sbv/news/press-release | Firecrawl | render=true

Notes
- Documented-only entries that are temporarily blocked remain included for link discovery; we only ingest pages returning HTTP 200.
- No secrets in logs; use env $(grep -Ev '^#|^$' app/.env | xargs) pattern for runs.

\n\n### Discovered Feeds (documented-only)

Authority | URL | Type
---|---|---
