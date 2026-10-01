"""Deterministic mock GitHub commit data source.

This module simulates a GitHub API by generating realistic but fully deterministic
commit data. It is the only component that needs to be replaced when integrating
the real GitHub API. All downstream Bronze/Silver/Gold logic is source-agnostic.

Replacing this module:
    Implement get_commits_for_date(processing_date: str) -> list[dict] using the
    real GitHub API (e.g., via PyGitHub or direct REST calls). The returned dicts
    must match the same field schema defined below.
"""

import hashlib
from datetime import date, datetime, timedelta

ORGANIZATION = "leapfrog"
REPOSITORIES = ["backend-api", "frontend-web", "data-platform", "mobile-app"]
DEVELOPERS = [
    ("Aryan Sharma", "aryan.sharma@lftechnology.com"),
    ("Priya Patel", "priya.patel@lftechnology.com"),
    ("Bikash Thapa", "bikash.thapa@lftechnology.com"),
    ("Sanjana Rai", "sanjana.rai@lftechnology.com"),
    ("Diwas Gurung", "diwas.gurung@lftechnology.com"),
]

# Historical data range covered by this mock source
DATA_START_DATE = date(2026, 9, 1)
DATA_END_DATE = date(2026, 9, 30)

_BRANCHES = ["main", "main", "main", "feature/auth", "feature/api-v2", "fix/null-check", "release/v2.1"]
_MESSAGES = [
    "Add user authentication flow",
    "Fix null pointer exception in API handler",
    "Refactor database connection pooling",
    "Update CI pipeline configuration",
    "Implement data validation layer",
    "Resolve merge conflicts with main",
    "Add unit tests for payment module",
    "Optimize query performance for dashboard",
    "Remove deprecated endpoints",
    "Update dependencies to latest versions",
    "Add structured logging for error tracking",
    "Fix memory leak in background worker",
    "Implement retry logic for external API calls",
    "Refactor authentication middleware",
    "Add integration tests for user onboarding flow",
    "Improve error messages in API responses",
    "Migrate config to environment variables",
    "Add pagination to listing endpoints",
]


def _h(key: str, modulo: int) -> int:
    """Deterministically map a string to [0, modulo) via SHA-256."""
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % modulo


def _commit_id(dt: date, repo: str, email: str, idx: int) -> str:
    return hashlib.sha1(f"{dt.isoformat()}-{repo}-{email}-{idx}".encode()).hexdigest()


def _active_on_date(dt: date, repo: str, email: str) -> bool:
    """Whether this developer commits to this repo on this date."""
    if dt.weekday() >= 5:  # weekend
        return _h(f"wknd-{dt}-{repo}-{email}", 10) < 2  # ~20% weekend activity
    return _h(f"work-{dt}-{repo}-{email}", 10) < 6  # ~60% weekday activity


def _commits_on_date(dt: date, repo: str, email: str) -> int:
    """Number of commits (1–4) when the developer is active."""
    return _h(f"cnt-{dt}-{repo}-{email}", 4) + 1


def _build_commit(dt: date, repo: str, dev_name: str, dev_email: str, idx: int) -> dict:
    cid = _commit_id(dt, repo, dev_email, idx)
    hour = _h(f"hr-{cid}", 10) + 8  # 08:00–17:00
    minute = _h(f"mn-{cid}", 60)
    committed_at = datetime(dt.year, dt.month, dt.day, hour, minute, 0)
    return {
        "commit_id": cid,
        "organization": ORGANIZATION,
        "repository": repo,
        "author": dev_name,
        "author_email": dev_email,
        "committed_at": committed_at.isoformat(),
        "message": _MESSAGES[_h(f"msg-{cid}", len(_MESSAGES))],
        "branch": _BRANCHES[_h(f"br-{cid}", len(_BRANCHES))],
        "additions": _h(f"add-{cid}", 300) + 1,
        "deletions": _h(f"del-{cid}", 150),
        "changed_files": _h(f"files-{cid}", 12) + 1,
    }


def _generate_for_date(dt: date) -> list[dict]:
    commits = []
    for repo in REPOSITORIES:
        for dev_name, dev_email in DEVELOPERS:
            if _active_on_date(dt, repo, dev_email):
                n = _commits_on_date(dt, repo, dev_email)
                for i in range(n):
                    commits.append(_build_commit(dt, repo, dev_name, dev_email, i))
    return commits


def get_commits_for_date(processing_date: str) -> list[dict]:
    """Return all commits authored on processing_date (YYYY-MM-DD).

    Returns [] for dates outside the mock's seeded range (DATA_START_DATE–DATA_END_DATE).
    Output is fully deterministic: identical inputs always produce identical output.
    This is the integration point — replace with a real GitHub API call to go live.
    """
    dt = date.fromisoformat(processing_date)
    if dt < DATA_START_DATE or dt > DATA_END_DATE:
        return []
    return _generate_for_date(dt)


def get_all_commits() -> list[dict]:
    """Return all commits across the full historical date range."""
    all_commits: list[dict] = []
    current = DATA_START_DATE
    while current <= DATA_END_DATE:
        all_commits.extend(_generate_for_date(current))
        current += timedelta(days=1)
    return all_commits
