import json, sys
from urllib.error import HTTPError, URLError
import urllib.request as u

def main():
    j=json.load(open('configs/firecrawl_seed.json', 'r', encoding='utf-8'))
    print('Authority,URL,Status,FinalURL')
    for s in j.get('startUrls', []):
        url=s.get('url'); label=s.get('label')
        if not url: continue
        req=u.Request(url, method='HEAD', headers={'User-Agent':'ASEANForge/1.0'})
        try:
            with u.urlopen(req, timeout=25) as r:
                print(f"{label},{url},{getattr(r,'status',200)},{r.geturl()}")
        except HTTPError as e:
            code = getattr(e, 'code', 'ERR')
            final = getattr(e, 'url', url)
            print(f"{label},{url},{code},{final}")
        except URLError as e:
            reason = getattr(e, 'reason', 'unknown')
            print(f"{label},{url},ERR:{reason},{url}")

if __name__ == '__main__':
    main()

