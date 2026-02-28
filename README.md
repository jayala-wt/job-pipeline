# Job Pipeline

A self-hosted job discovery and fit-scoring system. Pulls postings from multiple ATS APIs and job boards, scores each against a personal skill profile, and generates tailored resume variants.

## What it does

**Ingestion** (`ingest.py`) — pulls jobs from:
- Greenhouse board JSON API (configurable company slugs)
- Lever postings JSON API
- RemoteOK API
- Hacker News "Who's Hiring" threads (via Algolia HN Search)

**Scoring** (`scorer.py`) — scores each job against a weighted skill list:
- Keyword match on title + description with per-skill weights
- Title relevance boost for target roles
- Remote preference boost
- Stores matched/missing skills per job in SQLite

**Tailoring** (`tailor.py`) — two modes:
- **Rule-based**: annotates the resume template with matched/missing skills
- **LLM-prompt**: builds a structured prompt for Claude/GPT to generate a fully tailored variant (caller passes prompt to their LLM)

**Dashboard** (`app.py`) — Flask web UI:
- Job list with status, score, company, source filters
- Per-job detail with skill match breakdown
- REST API for ingestion triggers, status updates, notes, scoring

## Project layout

```
job-pipeline/
  app.py              # Flask Blueprint + standalone runner
  ingest.py           # Multi-source job ingestion
  scorer.py           # Keyword fit scoring
  tailor.py           # Resume tailoring (rule-based + LLM prompt)
  templates/          # Flask HTML templates
  profiles/           # (gitignored) master_profile.md + resume_template.md
  data/               # (gitignored) SQLite database
  output/             # (gitignored) Generated resume variants
```

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # edit paths as needed
mkdir -p data profiles output/resumes

# Add your profile files
cp your_master_profile.md profiles/master_profile.md
cp your_resume_template.md profiles/resume_template.md
```

## Run

```bash
# Initialize DB and start dashboard
python app.py

# Or run ingestion directly
python ingest.py all        # all sources
python ingest.py greenhouse # single source

# Score all unscored jobs
python scorer.py

# Tailor resume for a job (rule-based)
python -c "from tailor import generate_tailored_resume; print(generate_tailored_resume(1))"
```

Dashboard runs at `http://localhost:5050`.

## REST API

```
GET  /api/jobs                    # list jobs (status, source, min_score, limit filters)
GET  /api/jobs/<id>               # job detail
PUT  /api/jobs/<id>/status        # update status: new/to_apply/applied/interview/offer/rejected
PUT  /api/jobs/<id>/notes         # update notes
POST /api/ingest                  # trigger ingestion: {"source": "all|greenhouse|lever|remoteok|hn"}
POST /api/score/<id>              # score single job
POST /api/score/all               # score all unscored jobs
GET  /api/skills                  # list master skills
POST /api/skills                  # add skill: {"skill": "...", "category": "...", "weight": 1.0}
```

## Skill profile

Skills are seeded at DB init time (see `init_db()` in `app.py`) and can be added via the API. Each skill has a category and weight — higher weight means it contributes more to the fit score.

## LLM tailoring

`tailor.py` builds a structured prompt that instructs the LLM to reorder/rephrase existing resume content to match job language — without fabricating experience. Pass the returned `prompt` to any Claude/GPT endpoint.

## Database schema

- `jobs` — ingested postings with score, status, notes, applied_at
- `master_skills` — weighted skill list
- `job_skills` — per-job matched/missing skills
- `ingestion_runs` — run audit log

## Stack

- Python 3.10+
- Flask
- SQLite (WAL mode)
- Greenhouse / Lever / RemoteOK / HN Algolia APIs
