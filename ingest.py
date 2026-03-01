import os
"""
Job Ingestion Module
Pulls jobs from Greenhouse, Lever, Ashby JSON feeds, RemoteOK, and HN Who's Hiring.
"""
import json
import re
import sqlite3
import time
from datetime import datetime
from html import unescape
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

DB_PATH = os.environ.get('JOB_PIPELINE_DB', './data/jobs_pipeline.db')

# Curated list of company Greenhouse/Lever/Ashby board slugs.
# Add companies you're interested in tracking.
GREENHOUSE_BOARDS = [
    # Verified active Greenhouse board slugs
    # e.g. https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
    'stripe', 'figma', 'notion', 'plaid', 'databricks',
    'anthropic', 'reddit', 'discord', 'duolingo', 'cloudflare',
    'datadog', 'dbtlabs', 'montecarlodata',
    'airbyte', 'hashicorp', 'sourcegraph',
    'cockroachlabs', 'samsara', 'gusto',
    'airtable', 'webflow', 'vercel',
    'snyk', 'relativityhq', 'nianticlabs',
]

LEVER_BOARDS = [
    # https://api.lever.co/v0/postings/{company}
    'netflix', 'spotify',
]

# Remote job boards
REMOTEOK_URL = 'https://remoteok.com/api'

# HN Who's Hiring — scraped from Algolia HN Search API
HN_SEARCH_URL = 'https://hn.algolia.com/api/v1/search_by_date'

USER_AGENT = 'WanatuxJobPipeline/1.0 (personal use)'
REQUEST_TIMEOUT = 30


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _log_run(source: str) -> int:
    """Start an ingestion run log entry, return its ID."""
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO ingestion_runs (source) VALUES (?)', (source,)
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def _finish_run(run_id: int, found: int, new: int, error: str = None):
    conn = get_db()
    conn.execute(
        '''UPDATE ingestion_runs
           SET finished_at=?, jobs_found=?, jobs_new=?, status=?, error=?
           WHERE id=?''',
        (datetime.now().isoformat(), found, new,
         'error' if error else 'done', error, run_id)
    )
    conn.commit()
    conn.close()


def _upsert_job(job: Dict) -> bool:
    """Insert a job if URL not already in DB. Returns True if new."""
    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM jobs WHERE url = ?', (job['url'],)
    ).fetchone()
    if existing:
        conn.close()
        return False

    conn.execute('''
        INSERT INTO jobs (company, title, location, remote, url, source,
                          ats_type, description_text, salary_min, salary_max)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        job.get('company', ''),
        job.get('title', ''),
        job.get('location', ''),
        1 if job.get('remote', True) else 0,
        job['url'],
        job.get('source', ''),
        job.get('ats_type', ''),
        job.get('description_text', ''),
        job.get('salary_min'),
        job.get('salary_max'),
    ))
    conn.commit()
    conn.close()
    return True


# ---------------------------------------------------------------------------
# Greenhouse
# ---------------------------------------------------------------------------

def ingest_greenhouse(slug: str) -> List[Dict]:
    """Fetch jobs from a Greenhouse board JSON API."""
    url = f'https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true'
    try:
        resp = requests.get(url, headers={'User-Agent': USER_AGENT},
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f'  [greenhouse/{slug}] request error: {e}')
        return []

    data = resp.json()
    jobs_raw = data.get('jobs', [])
    results = []

    for j in jobs_raw:
        loc = j.get('location', {}).get('name', '')
        content = j.get('content', '')
        # Strip HTML tags for plain text
        desc = re.sub(r'<[^>]+>', ' ', unescape(content or ''))
        desc = re.sub(r'\s+', ' ', desc).strip()

        results.append({
            'company': slug.replace('-', ' ').title(),
            'title': j.get('title', ''),
            'location': loc,
            'remote': 'remote' in loc.lower(),
            'url': j.get('absolute_url', ''),
            'source': 'greenhouse',
            'ats_type': 'greenhouse',
            'description_text': desc[:10000],
        })
    return results


# ---------------------------------------------------------------------------
# Lever
# ---------------------------------------------------------------------------

def ingest_lever(company: str) -> List[Dict]:
    """Fetch jobs from a Lever postings JSON API."""
    url = f'https://api.lever.co/v0/postings/{company}?mode=json'
    try:
        resp = requests.get(url, headers={'User-Agent': USER_AGENT},
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f'  [lever/{company}] request error: {e}')
        return []

    data = resp.json()
    results = []

    for j in data:
        loc = j.get('categories', {}).get('location', '')
        desc_parts = []
        for section in j.get('lists', []):
            desc_parts.append(section.get('text', ''))
            for item in section.get('content', '').split('<li>'):
                clean = re.sub(r'<[^>]+>', '', item).strip()
                if clean:
                    desc_parts.append(clean)
        desc = j.get('descriptionPlain', '') + '\n' + '\n'.join(desc_parts)

        results.append({
            'company': company.replace('-', ' ').title(),
            'title': j.get('text', ''),
            'location': loc,
            'remote': 'remote' in (loc or '').lower(),
            'url': j.get('hostedUrl', ''),
            'source': 'lever',
            'ats_type': 'lever',
            'description_text': desc[:10000],
        })
    return results


# ---------------------------------------------------------------------------
# RemoteOK
# ---------------------------------------------------------------------------

def ingest_remoteok() -> List[Dict]:
    """Fetch jobs from RemoteOK JSON API."""
    try:
        resp = requests.get(REMOTEOK_URL,
                            headers={'User-Agent': USER_AGENT},
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f'  [remoteok] request error: {e}')
        return []

    data = resp.json()
    results = []

    for j in data:
        if isinstance(j, dict) and j.get('slug'):
            salary_min = None
            salary_max = None
            if j.get('salary_min'):
                try:
                    salary_min = int(j['salary_min'])
                except (ValueError, TypeError):
                    pass
            if j.get('salary_max'):
                try:
                    salary_max = int(j['salary_max'])
                except (ValueError, TypeError):
                    pass

            desc = j.get('description', '')
            desc = re.sub(r'<[^>]+>', ' ', unescape(desc))
            desc = re.sub(r'\s+', ' ', desc).strip()

            results.append({
                'company': j.get('company', ''),
                'title': j.get('position', ''),
                'location': j.get('location', 'Remote'),
                'remote': True,
                'url': j.get('url', ''),
                'source': 'remoteok',
                'ats_type': 'remoteok',
                'description_text': desc[:10000],
                'salary_min': salary_min,
                'salary_max': salary_max,
            })
    return results


# ---------------------------------------------------------------------------
# Hacker News Who's Hiring
# ---------------------------------------------------------------------------

def ingest_hn_whos_hiring(max_comments: int = 500) -> List[Dict]:
    """Fetch the latest HN Who's Hiring thread comments via Algolia API."""
    # Find the latest "Who is hiring?" post
    try:
        resp = requests.get(HN_SEARCH_URL, params={
            'query': '"Who is hiring?"',
            'tags': 'story,author_whoishiring',
            'hitsPerPage': 1,
        }, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        hits = resp.json().get('hits', [])
    except requests.RequestException as e:
        print(f'  [hn] search error: {e}')
        return []

    if not hits:
        print('  [hn] no Who is Hiring thread found')
        return []

    story_id = hits[0].get('objectID')
    story_title = hits[0].get('title', '')
    print(f'  [hn] found thread: {story_title} (id={story_id})')

    # Fetch comments
    results = []
    page = 0
    fetched = 0

    while fetched < max_comments:
        try:
            resp = requests.get(HN_SEARCH_URL, params={
                'tags': f'comment,story_{story_id}',
                'hitsPerPage': 100,
                'page': page,
            }, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            comments = resp.json().get('hits', [])
        except requests.RequestException as e:
            print(f'  [hn] comment fetch error page {page}: {e}')
            break

        if not comments:
            break

        for c in comments:
            text = c.get('comment_text', '')
            if not text or len(text) < 50:
                continue

            # Try to parse company | role | location from first line
            plain = re.sub(r'<[^>]+>', '\n', unescape(text))
            lines = [l.strip() for l in plain.split('\n') if l.strip()]
            if not lines:
                continue

            first_line = lines[0]
            # Common format: "Company | Role | Location | ..."
            parts = [p.strip() for p in first_line.split('|')]

            company = parts[0] if len(parts) >= 1 else 'Unknown'
            title = parts[1] if len(parts) >= 2 else first_line
            location = parts[2] if len(parts) >= 3 else ''

            is_remote = bool(re.search(r'\bremote\b', first_line, re.IGNORECASE))

            hn_url = f'https://news.ycombinator.com/item?id={c.get("objectID", "")}'

            results.append({
                'company': company[:200],
                'title': title[:300],
                'location': location[:200],
                'remote': is_remote,
                'url': hn_url,
                'source': 'hn_whos_hiring',
                'ats_type': 'hn',
                'description_text': plain[:10000],
            })

        fetched += len(comments)
        page += 1
        time.sleep(0.5)  # Be polite to Algolia

    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_ingestion(source: str = 'all') -> Dict:
    """Run ingestion for the specified source(s). Returns summary."""
    run_id = _log_run(source)
    total_found = 0
    total_new = 0
    errors = []

    try:
        if source in ('all', 'greenhouse'):
            for slug in GREENHOUSE_BOARDS:
                print(f'  Ingesting greenhouse/{slug}...')
                jobs = ingest_greenhouse(slug)
                new = sum(1 for j in jobs if _upsert_job(j))
                total_found += len(jobs)
                total_new += new
                print(f'    → {len(jobs)} found, {new} new')
                time.sleep(0.3)

        if source in ('all', 'lever'):
            for company in LEVER_BOARDS:
                print(f'  Ingesting lever/{company}...')
                jobs = ingest_lever(company)
                new = sum(1 for j in jobs if _upsert_job(j))
                total_found += len(jobs)
                total_new += new
                print(f'    → {len(jobs)} found, {new} new')
                time.sleep(0.3)

        if source in ('all', 'remoteok'):
            print('  Ingesting remoteok...')
            jobs = ingest_remoteok()
            new = sum(1 for j in jobs if _upsert_job(j))
            total_found += len(jobs)
            total_new += new
            print(f'    → {len(jobs)} found, {new} new')

        if source in ('all', 'hn'):
            print('  Ingesting HN Who\'s Hiring...')
            jobs = ingest_hn_whos_hiring()
            new = sum(1 for j in jobs if _upsert_job(j))
            total_found += len(jobs)
            total_new += new
            print(f'    → {len(jobs)} found, {new} new')

    except Exception as e:
        errors.append(str(e))

    _finish_run(run_id, total_found, total_new,
                '; '.join(errors) if errors else None)

    return {
        'run_id': run_id,
        'source': source,
        'jobs_found': total_found,
        'jobs_new': total_new,
        'errors': errors,
    }


if __name__ == '__main__':
    # Quick CLI test
    import sys
    source = sys.argv[1] if len(sys.argv) > 1 else 'all'
    result = run_ingestion(source)
    print(json.dumps(result, indent=2))
