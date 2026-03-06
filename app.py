"""
Jobs Pipeline Blueprint
Job ingestion, fit scoring, resume tailoring, and application tracking.
"""
import json
import sqlite3
import os
from datetime import datetime
from flask import Blueprint, Flask, render_template, request, jsonify
import os

jobs_pipeline_bp = Blueprint(
    'jobs_pipeline',
    __name__,
    url_prefix='/jobs',
    template_folder='templates',
)

DB_PATH = os.environ.get('JOB_PIPELINE_DB', './data/jobs_pipeline.db')


def init_db():
    """Initialize the jobs pipeline database."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute('PRAGMA journal_mode=WAL')

    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL,
            title TEXT NOT NULL,
            location TEXT,
            remote INTEGER DEFAULT 1,
            url TEXT,
            source TEXT NOT NULL,
            ats_type TEXT,
            date_found TEXT DEFAULT CURRENT_TIMESTAMP,
            description_text TEXT,
            salary_min INTEGER,
            salary_max INTEGER,
            status TEXT DEFAULT 'new',
            notes TEXT,
            match_score REAL,
            match_details TEXT,
            resume_path TEXT,
            applied_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS master_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill TEXT NOT NULL UNIQUE,
            category TEXT,
            weight REAL DEFAULT 1.0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS job_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            skill TEXT NOT NULL,
            matched INTEGER DEFAULT 0,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            jobs_found INTEGER DEFAULT 0,
            jobs_new INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            error TEXT
        )
    ''')

    # Indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_jobs_score ON jobs(match_score)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_url ON jobs(url)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_job_skills_job ON job_skills(job_id)')

    # Seed default master skills if empty
    count = c.execute('SELECT COUNT(*) FROM master_skills').fetchone()[0]
    if count == 0:
        default_skills = [
            ('Python', 'programming', 1.0),
            ('SQL', 'programming', 1.0),
            ('SQLite', 'programming', 0.8),
            ('BigQuery', 'programming', 0.7),
            ('pandas', 'programming', 0.9),
            ('NumPy', 'programming', 0.7),
            ('scikit-learn', 'ml', 1.0),
            ('TF-IDF', 'ml', 0.8),
            ('LDA', 'ml', 0.7),
            ('NMF', 'ml', 0.7),
            ('Topic Modeling', 'ml', 0.8),
            ('NLP', 'ml', 0.9),
            ('Classification', 'ml', 0.8),
            ('CLIP', 'ml', 0.6),
            ('LLM', 'ml', 0.9),
            ('Claude', 'ml', 0.5),
            ('GPT', 'ml', 0.5),
            ('Ollama', 'ml', 0.5),
            ('Sentiment Analysis', 'ml', 0.7),
            ('Machine Learning', 'ml', 1.0),
            ('Data Modeling', 'analytics', 0.9),
            ('Analytics Engineering', 'analytics', 1.0),
            ('Power BI', 'visualization', 0.8),
            ('Tableau', 'visualization', 0.8),
            ('Looker', 'visualization', 0.7),
            ('A/B Testing', 'analytics', 0.8),
            ('Experimentation', 'analytics', 0.7),
            ('Six Sigma', 'analytics', 0.5),
            ('H3', 'geo', 0.6),
            ('Geospatial', 'geo', 0.6),
            ('Google Apps Script', 'automation', 0.5),
            ('REST API', 'infrastructure', 0.7),
            ('APIs', 'infrastructure', 0.7),
            ('Flask', 'infrastructure', 0.7),
            ('Linux', 'infrastructure', 0.6),
            ('Git', 'infrastructure', 0.6),
            ('Docker', 'infrastructure', 0.6),
            ('Streamlit', 'visualization', 0.7),
            ('GCP', 'infrastructure', 0.6),
            ('AWS', 'infrastructure', 0.5),
            ('ETL', 'analytics', 0.8),
            ('Data Pipeline', 'analytics', 0.9),
            ('Automation', 'automation', 0.8),
            ('MCP', 'infrastructure', 0.5),
            ('Cloudflare', 'infrastructure', 0.4),
        ]
        c.executemany(
            'INSERT INTO master_skills (skill, category, weight) VALUES (?, ?, ?)',
            default_skills
        )

    conn.commit()
    conn.close()


def get_db():
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@jobs_pipeline_bp.route('/')
def dashboard():
    """Jobs pipeline dashboard."""
    conn = get_db()

    stats = {
        'total': conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],
        'new': conn.execute("SELECT COUNT(*) FROM jobs WHERE status='new'").fetchone()[0],
        'applied': conn.execute("SELECT COUNT(*) FROM jobs WHERE status='applied'").fetchone()[0],
        'interview': conn.execute("SELECT COUNT(*) FROM jobs WHERE status='interview'").fetchone()[0],
        'rejected': conn.execute("SELECT COUNT(*) FROM jobs WHERE status='rejected'").fetchone()[0],
    }

    # Recent jobs with optional filters
    status_filter = request.args.get('status', '')
    source_filter = request.args.get('source', '')
    min_score = request.args.get('min_score', '')
    search = request.args.get('q', '')

    query = 'SELECT * FROM jobs WHERE 1=1'
    params = []

    if status_filter:
        query += ' AND status = ?'
        params.append(status_filter)
    if source_filter:
        query += ' AND source = ?'
        params.append(source_filter)
    if min_score:
        query += ' AND match_score >= ?'
        params.append(float(min_score))
    if search:
        query += ' AND (title LIKE ? OR company LIKE ? OR description_text LIKE ?)'
        term = f'%{search}%'
        params.extend([term, term, term])

    query += ' ORDER BY match_score DESC NULLS LAST, date_found DESC LIMIT 200'

    jobs = [dict(r) for r in conn.execute(query, params).fetchall()]

    sources = [r[0] for r in conn.execute(
        'SELECT DISTINCT source FROM jobs ORDER BY source'
    ).fetchall()]

    conn.close()

    return render_template(
        'jobs_pipeline/dashboard.html',
        stats=stats,
        jobs=jobs,
        sources=sources,
        filters={
            'status': status_filter,
            'source': source_filter,
            'min_score': min_score,
            'q': search,
        }
    )


@jobs_pipeline_bp.route('/<int:job_id>')
def job_detail(job_id):
    """Single job detail view."""
    conn = get_db()
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    if not job:
        conn.close()
        return 'Job not found', 404
    job = dict(job)

    skills = [dict(r) for r in conn.execute(
        'SELECT * FROM job_skills WHERE job_id = ?', (job_id,)
    ).fetchall()]
    conn.close()

    return render_template(
        'jobs_pipeline/job_detail.html',
        job=job,
        skills=skills,
    )


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@jobs_pipeline_bp.route('/api/jobs', methods=['GET'])
def api_list_jobs():
    """List jobs with optional filters."""
    conn = get_db()
    status = request.args.get('status')
    source = request.args.get('source')
    limit = request.args.get('limit', 100, type=int)

    query = 'SELECT * FROM jobs WHERE 1=1'
    params = []
    if status:
        query += ' AND status = ?'
        params.append(status)
    if source:
        query += ' AND source = ?'
        params.append(source)
    query += ' ORDER BY match_score DESC NULLS LAST, date_found DESC LIMIT ?'
    params.append(limit)

    jobs = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()
    return jsonify(jobs)


@jobs_pipeline_bp.route('/api/jobs/<int:job_id>', methods=['GET'])
def api_get_job(job_id):
    """Get single job."""
    conn = get_db()
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()
    if not job:
        return jsonify({'error': 'not found'}), 404
    return jsonify(dict(job))


@jobs_pipeline_bp.route('/api/jobs/<int:job_id>/status', methods=['PUT'])
def api_update_status(job_id):
    """Update job status (new/applied/interview/rejected/passed)."""
    data = request.json or {}
    new_status = data.get('status')
    valid = ('new', 'to_apply', 'applied', 'interview', 'offer', 'rejected', 'passed')
    if new_status not in valid:
        return jsonify({'error': f'status must be one of {valid}'}), 400

    conn = get_db()
    now = datetime.now().isoformat()
    applied_at = now if new_status == 'applied' else None

    if applied_at:
        conn.execute(
            'UPDATE jobs SET status=?, applied_at=?, updated_at=? WHERE id=?',
            (new_status, applied_at, now, job_id)
        )
    else:
        conn.execute(
            'UPDATE jobs SET status=?, updated_at=? WHERE id=?',
            (new_status, now, job_id)
        )
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'status': new_status})


@jobs_pipeline_bp.route('/api/jobs/<int:job_id>/notes', methods=['PUT'])
def api_update_notes(job_id):
    """Update notes for a job."""
    data = request.json or {}
    conn = get_db()
    conn.execute(
        'UPDATE jobs SET notes=?, updated_at=? WHERE id=?',
        (data.get('notes', ''), datetime.now().isoformat(), job_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@jobs_pipeline_bp.route('/api/skills', methods=['GET'])
def api_list_skills():
    """List master skills."""
    conn = get_db()
    skills = [dict(r) for r in conn.execute(
        'SELECT * FROM master_skills ORDER BY category, skill'
    ).fetchall()]
    conn.close()
    return jsonify(skills)


@jobs_pipeline_bp.route('/api/skills', methods=['POST'])
def api_add_skill():
    """Add a master skill."""
    data = request.json or {}
    skill = data.get('skill', '').strip()
    if not skill:
        return jsonify({'error': 'skill required'}), 400
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO master_skills (skill, category, weight) VALUES (?, ?, ?)',
            (skill, data.get('category', 'general'), data.get('weight', 1.0))
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': 'skill already exists'}), 409
    conn.close()
    return jsonify({'ok': True}), 201


@jobs_pipeline_bp.route('/api/ingest', methods=['POST'])
def api_trigger_ingest():
    """Trigger an ingestion run for a source (runs in background thread)."""
    import threading
    from ingest import run_ingestion
    data = request.json or {}
    source = data.get('source', 'all')

    def _run():
        try:
            run_ingestion(source)
        except Exception as e:
            print(f'Ingestion error: {e}')

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({'ok': True, 'message': f'Ingestion started for {source}', 'async': True})


@jobs_pipeline_bp.route('/api/score/<int:job_id>', methods=['POST'])
def api_score_job(job_id):
    """Score a single job against master profile."""
    from scorer import score_job
    try:
        result = score_job(job_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@jobs_pipeline_bp.route('/api/score/all', methods=['POST'])
def api_score_all():
    """Score all unscored jobs."""
    from scorer import score_all_jobs
    try:
        result = score_all_jobs()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def create_app():
    """Create standalone Flask application."""
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.secret_key = os.environ.get('SECRET_KEY', 'job-pipeline-dev-key')
    app.register_blueprint(jobs_pipeline_bp, url_prefix='/')
    return app


if __name__ == '__main__':
    app = create_app()
    with app.app_context():
        init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5050)), debug=False)
