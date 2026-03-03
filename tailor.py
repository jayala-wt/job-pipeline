"""
Resume Tailor Module
Generates a tailored resume variant from the Master Profile + Version A template,
focused on keywords and emphasis from a specific job description.

Uses LLM orchestration if available, falls back to rule-based keyword emphasis.
"""
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

DB_PATH = os.environ.get('JOB_PIPELINE_DB', './data/jobs_pipeline.db')
MASTER_PROFILE_PATH = Path(os.environ.get('MASTER_PROFILE_PATH', './profiles/master_profile.md'))
RESUME_VERSION_A_PATH = Path(os.environ.get('RESUME_TEMPLATE_PATH', './profiles/resume_template.md'))
OUTPUT_DIR = Path(os.environ.get('RESUME_OUTPUT_DIR', './output/resumes'))


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _load_text(path: Path) -> str:
    return path.read_text(encoding='utf-8')


def _sanitize_filename(text: str) -> str:
    """Create a safe filename from company + role."""
    clean = re.sub(r'[^a-zA-Z0-9\s\-]', '', text)
    clean = re.sub(r'\s+', '_', clean.strip())
    return clean[:60].lower()


def build_tailoring_prompt(job: Dict, master_profile: str, resume_template: str) -> str:
    """
    Build the LLM prompt for resume tailoring.
    This can be sent to Claude/GPT via the existing Devloop orchestration.
    """
    matched = []
    missing = []
    if job.get('match_details'):
        try:
            details = json.loads(job['match_details'])
            matched = details.get('matched_skills', [])
            missing = details.get('missing_skills', [])
        except (json.JSONDecodeError, TypeError):
            pass

    prompt = f"""You are a resume tailoring assistant. Your job is to adapt an existing resume
to better match a specific job description. You must ONLY use experience, skills, and
accomplishments that already exist in the Master Profile — never fabricate or hallucinate
new experience.

## Rules
1. Keep the same structure as the Resume Template below.
2. Adjust the headline/summary to emphasize skills the JD values most.
3. Reorder or rephrase bullet points to mirror the JD's language (e.g., if JD says
   "data pipelines", use that phrase instead of "ETL workflows" if both describe the same work).
4. In Core Skills, move matched skills higher and add any from Master Profile that match
   the JD but were absent from the template.
5. Do NOT add skills, roles, or accomplishments not present in the Master Profile.
6. Output in clean Markdown format.

## Job Description
Company: {job.get('company', 'Unknown')}
Title: {job.get('title', 'Unknown')}
Location: {job.get('location', '')}

{job.get('description_text', '')[:5000]}

## Matched Skills (already on resume): {', '.join(matched)}
## Missing Skills (in JD but not matched): {', '.join(missing)}

## Master Profile (authoritative source of truth)
{master_profile[:8000]}

## Resume Template (structure to follow)
{resume_template}

## Output
Return ONLY the tailored resume in Markdown. No commentary.
"""
    return prompt


def generate_tailored_resume(job_id: int, use_llm: bool = False) -> Dict:
    """
    Generate a tailored resume for a specific job.

    If use_llm=True, builds the prompt and returns it (caller sends to LLM).
    If use_llm=False, creates a rule-based variant emphasizing matched keywords.
    """
    conn = get_db()
    row = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()
    if not row:
        raise ValueError(f'Job {job_id} not found')
    job = dict(row)

    master_profile = _load_text(MASTER_PROFILE_PATH)
    resume_template = _load_text(RESUME_VERSION_A_PATH)

    if use_llm:
        # Return the prompt for the caller to send through their LLM pipeline
        prompt = build_tailoring_prompt(job, master_profile, resume_template)
        return {
            'job_id': job_id,
            'mode': 'llm_prompt',
            'prompt': prompt,
            'prompt_chars': len(prompt),
        }

    # Rule-based tailoring: produce an annotated version
    company_slug = _sanitize_filename(job.get('company', 'unknown'))
    role_slug = _sanitize_filename(job.get('title', 'role'))
    date_str = datetime.now().strftime('%Y%m%d')
    filename = f'{company_slug}_{role_slug}_{date_str}.md'

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / filename

    # Parse match details
    matched_skills = []
    missing_skills = []
    if job.get('match_details'):
        try:
            details = json.loads(job['match_details'])
            matched_skills = details.get('matched_skills', [])
            missing_skills = details.get('missing_skills', [])
        except (json.JSONDecodeError, TypeError):
            pass

    # Build a tailoring header + the original resume with annotations
    header = f"""<!-- TAILORED RESUME -->
<!-- Job: {job.get('title', '')} @ {job.get('company', '')} -->
<!-- Score: {job.get('match_score', 'N/A')} -->
<!-- Matched: {', '.join(matched_skills)} -->
<!-- Missing from JD: {', '.join(missing_skills)} -->
<!-- Generated: {datetime.now().isoformat()} -->
<!-- NOTE: Review and manually adjust before submitting -->

"""
    # Write the annotated resume
    tailored = header + resume_template
    output_path.write_text(tailored, encoding='utf-8')

    # Update DB with resume path
    conn = get_db()
    conn.execute(
        'UPDATE jobs SET resume_path=?, updated_at=? WHERE id=?',
        (str(output_path), datetime.now().isoformat(), job_id)
    )
    conn.commit()
    conn.close()

    return {
        'job_id': job_id,
        'mode': 'rule_based',
        'output_path': str(output_path),
        'matched_skills': matched_skills,
        'missing_skills': missing_skills,
        'note': 'Review annotations at top of file and adjust emphasis manually.',
    }
