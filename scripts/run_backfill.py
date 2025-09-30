#!/usr/bin/env python3
import os
import re
import subprocess
import zipfile
from datetime import datetime, timezone
from dotenv import load_dotenv

SOURCE_FILTER_VAL = 'ASEAN,PDPC,BOT,BSP,SBV,MIC,OJK,BI,SC,MAS,IMDA,MCMC,DICT'
LATEST_DIR = os.path.join('data','output','validation','latest')
DELIVER_DIR = 'deliverables'


def iso_now():
    return datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')


def run(cmd, env=None, out_path=None):
    os.makedirs(LATEST_DIR, exist_ok=True)
    if out_path:
        with open(out_path, 'wb') as fh:
            return subprocess.run(cmd, check=False, stdout=fh, stderr=subprocess.STDOUT, env=env)
    else:
        return subprocess.run(cmd, check=False, env=env)


def grep_items_new(log_path):
    pat = re.compile(rb"items_new=\d+")
    try:
        with open(log_path, 'rb') as f:
            for line in f:
                m = pat.search(line)
                if m:
                    last = m.group(0)
        return last.decode('utf-8') if 'last' in locals() else ''
    except Exception:
        return ''


def write_idem_proof(from_log, out_txt):
    line = ''
    try:
        with open(from_log, 'r', encoding='utf-8') as fh:
            lines = fh.readlines()
        for L in reversed(lines):
            if 'Ingest done.' in L and 'items_new=' in L:
                line = L.strip()
                break
    except Exception:
        pass
    with open(out_txt, 'w', encoding='utf-8') as fh:
        if line:
            fh.write(line + "\n")
        else:
            fh.write('[n/a] idempotency line not found\n')


def db_proofs(db_url):
    # Authority counts
    run(['psql', db_url, '-c', "SELECT authority, COUNT(*) FROM events GROUP BY 1 ORDER BY 1;"], out_path=os.path.join(LATEST_DIR, 'db_auth_counts_backfill.txt'))
    # Totals
    ev = subprocess.run(['psql', db_url, '-t', '-A', '-c', 'SELECT count(*) FROM events;'], stdout=subprocess.PIPE)
    dv = subprocess.run(['psql', db_url, '-t', '-A', '-c', 'SELECT count(*) FROM documents;'], stdout=subprocess.PIPE)
    with open(os.path.join(LATEST_DIR, 'db_totals_backfill.txt'), 'w', encoding='utf-8') as fh:
        fh.write('events_cnt\t' + (ev.stdout.decode().strip()) + '\n')
        fh.write('documents_cnt\t' + (dv.stdout.decode().strip()) + '\n')


def make_snapshot():
    ts = iso_now()
    os.makedirs(DELIVER_DIR, exist_ok=True)
    bundle = os.path.join(DELIVER_DIR, f'policy_tape_snapshot_{ts}.zip')
    with zipfile.ZipFile(bundle, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        # include latest/*.txt, *.csv, *.log and vendor packet
        for name in os.listdir(LATEST_DIR):
            if any(name.endswith(ext) for ext in ('.txt','.csv','.log','.md')):
                z.write(os.path.join(LATEST_DIR, name), arcname=name)
    with open(os.path.join(LATEST_DIR, 'snapshot_path_backfill.txt'), 'w', encoding='utf-8') as fh:
        fh.write(bundle + '\n')
    return bundle


def append_roadmap(db_totals_path, bundle_path):
    # Read counts
    events = documents = '0'
    try:
        with open(db_totals_path, 'r', encoding='utf-8') as fh:
            for line in fh:
                if line.startswith('events_cnt'):
                    events = line.split('\t',1)[1].strip()
                if line.startswith('documents_cnt'):
                    documents = line.split('\t',1)[1].strip()
    except Exception:
        pass
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        with open('docs/ROADMAP.md','a',encoding='utf-8') as f:
            f.write(f"\n- {ts}: events={events}, documents={documents}, zip={bundle_path} (Automated Backfill)\n")
    except Exception:
        pass


def main():
    load_dotenv('app/.env')
    db_url = os.getenv('NEON_DATABASE_URL')
    if not db_url:
        raise SystemExit('NEON_DATABASE_URL not set; configure app/.env')

    # Vendor flags checkpoint
    run(['.venv/bin/python','scripts/check_vendor_flags.py'], out_path=os.path.join(LATEST_DIR,'vendor_flags.log'))

    env = os.environ.copy()
    env['SOURCE_FILTER'] = SOURCE_FILTER_VAL

    # Phase A
    log_a = os.path.join(LATEST_DIR, 'can_phaseA_run.log')
    run(['.venv/bin/python','scripts/ingest_sources.py','--config','config/sources.yaml','--limit-per-source','10','--max-depth','1'], env=env, out_path=log_a)
    log_a2 = os.path.join(LATEST_DIR, 'can_phaseA_rerun.log')
    run(['.venv/bin/python','scripts/ingest_sources.py','--config','config/sources.yaml','--limit-per-source','10','--max-depth','1'], env=env, out_path=log_a2)
    write_idem_proof(log_a2, os.path.join(LATEST_DIR,'idempotency_can_phaseA.txt'))

    # Phase B
    log_b = os.path.join(LATEST_DIR, 'can_phaseB_run.log')
    run(['.venv/bin/python','scripts/ingest_sources.py','--config','config/sources.yaml','--limit-per-source','50','--max-depth','2','--pdf-only'], env=env, out_path=log_b)
    log_b2 = os.path.join(LATEST_DIR, 'can_phaseB_rerun.log')
    run(['.venv/bin/python','scripts/ingest_sources.py','--config','config/sources.yaml','--limit-per-source','50','--max-depth','2','--pdf-only'], env=env, out_path=log_b2)
    write_idem_proof(log_b2, os.path.join(LATEST_DIR,'idempotency_can_phaseB.txt'))

    # DB proofs
    db_proofs(db_url)

    # Snapshot
    bundle = make_snapshot()

    # Append ROADMAP
    append_roadmap(os.path.join(LATEST_DIR,'db_totals_backfill.txt'), bundle)

    print('Backfill repeatable job completed.')

if __name__ == '__main__':
    main()

