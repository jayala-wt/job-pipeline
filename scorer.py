"""
Job Fit Scorer
Scores job descriptions against the master skill profile using
keyword matching and TF-IDF similarity.
"""
import re
import sqlite3
import json
from collections import Counter
from datetime import datetime
from typing import Dict, List, Tuple

DB_PATH = os.environ.get('JOB_PIPELINE_DB', './data/jobs_pipeline.db')

# Path to the master profile markdown (used for TF-IDF baseline)
MASTER_PROFILE_PATH = os.environ.get('MASTER_PROFILE_PATH', './profiles/master_profile.md')
RESUME_VERSION_A_PATH = os.environ.get('RESUME_TEMPLATE_PATH', './profiles/resume_template.md')


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _load_master_skills() -> List[Dict]:
    """Load master skills from DB."""
    conn = get_db()
    rows = conn.execute('SELECT skill, category, weight FROM master_skills').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _normalize(text: str) -> str:
    """Lowercase, strip punctuation for matching."""
    return re.sub(r'[^a-z0-9\s/\-+#.]', '', text.lower())


def _extract_keywords(text: str) -> Counter:
    """Extract word tokens from text."""
    normalized = _normalize(text)
    words = normalized.split()
    return Counter(words)


def _keyword_match_score(job_text: str, skills: List[Dict]) -> Tuple[float, List[str], List[str]]:
    """
    Compute keyword match score.
    Returns (score 0-100, matched_skills, missing_skills).
    """
    jt = _normalize(job_text)
    matched = []
    missing = []
    weighted_hits = 0.0
    total_weight = 0.0

    for s in skills:
        skill_lower = s['skill'].lower()
        weight = s.get('weight', 1.0)
        total_weight += weight

        # Try exact phrase match first, then individual word match
        if skill_lower in jt:
            matched.append(s['skill'])
            weighted_hits += weight
        else:
            # Try matching individual words of multi-word skills
            skill_words = skill_lower.split()
            if len(skill_words) > 1 and all(w in jt for w in skill_words):
                matched.append(s['skill'])
                weighted_hits += weight * 0.8  # Partial credit
            else:
                missing.append(s['skill'])

    score = (weighted_hits / total_weight * 100) if total_weight > 0 else 0
    return round(score, 1), matched, missing


def _title_relevance_boost(title: str) -> float:
    """Boost score based on title relevance to target roles."""
    title_lower = title.lower()
    high_match = [
        'analytics engineer', 'data engineer', 'ml engineer',
        'machine learning', 'ai engineer', 'data scientist',
        'analytics', 'nlp engineer',
    ]
    medium_match = [
        'python', 'backend engineer', 'software engineer',
        'data analyst', 'platform engineer', 'automation',
    ]

    for phrase in high_match:
        if phrase in title_lower:
            return 15.0
    for phrase in medium_match:
        if phrase in title_lower:
            return 8.0
    return 0.0


def _remote_boost(job: Dict) -> float:
    """Small boost for explicitly remote jobs."""
    if job.get('remote'):
        return 5.0
    location = (job.get('location') or '').lower()
    if 'remote' in location:
        return 5.0
    return 0.0


def score_job(job_id: int) -> Dict:
    """Score a single job and update the DB."""
    conn = get_db()
    row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    if not row:
        conn.close()
        raise ValueError(f'Job {job_id} not found')
    job = dict(row)
    conn.close()

    skills = _load_master_skills()
    desc = job.get('description_text', '') or ''
    title = job.get('title', '') or ''

    # Combine title + description for scoring
    full_text = f"{title} {desc}"

    base_score, matched, missing = _keyword_match_score(full_text, skills)
    title_boost = _title_relevance_boost(title)
    remote_boost = _remote_boost(job)

    final_score = min(100.0, base_score + title_boost + remote_boost)

    match_details = json.dumps({
        'base_score': base_score,
        'title_boost': title_boost,
        'remote_boost': remote_boost,
        'matched_skills': matched,
        'missing_skills': missing,
        'matched_count': len(matched),
        'total_skills': len(skills),
    })

    # Update DB
    conn = get_db()
    conn.execute(
        '''UPDATE jobs SET match_score=?, match_details=?, updated_at=?
           WHERE id=?''',
        (final_score, match_details, datetime.now().isoformat(), job_id)
    )

    # Update job_skills table
    conn.execute('DELETE FROM job_skills WHERE job_id = ?', (job_id,))
    for s in matched:
        conn.execute(
            'INSERT INTO job_skills (job_id, skill, matched) VALUES (?, ?, 1)',
            (job_id, s)
        )
    for s in missing:
        conn.execute(
            'INSERT INTO job_skills (job_id, skill, matched) VALUES (?, ?, 0)',
            (job_id, s)
        )

    conn.commit()
    conn.close()

    return {
        'job_id': job_id,
        'score': final_score,
        'matched': matched,
        'missing': missing,
        'details': json.loads(match_details),
    }


def score_all_jobs(rescore: bool = False) -> Dict:
    """Score all jobs. If rescore=False, only score unscored jobs."""
    conn = get_db()
    if rescore:
        rows = conn.execute('SELECT id FROM jobs').fetchall()
    else:
        rows = conn.execute(
            'SELECT id FROM jobs WHERE match_score IS NULL'
        ).fetchall()
    conn.close()

    scored = 0
    errors = 0
    for row in rows:
        try:
            score_job(row['id'])
            scored += 1
        except Exception as e:
            print(f'  Error scoring job {row["id"]}: {e}')
            errors += 1

    return {
        'scored': scored,
        'errors': errors,
        'total_candidates': len(rows),
    }


if __name__ == '__main__':
    result = score_all_jobs()
    print(json.dumps(result, indent=2))
