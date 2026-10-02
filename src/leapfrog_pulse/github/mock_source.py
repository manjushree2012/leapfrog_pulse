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


def get_commits_for_repo_and_date(repo_name: str, org: str, processing_date: str) -> list[dict]:
    """Return commits for a specific (org, repo) on processing_date.

    Unlike get_commits_for_date(), this accepts any repo name — including repos
    discovered from Vyaguta — and is not limited to the static REPOSITORIES list.
    Output is fully deterministic for any (repo_name, org, processing_date) triple.
    This is the integration point for the project-driven pipeline; replace with a
    real GitHub API call scoped to the repository to go live.
    """
    dt = date.fromisoformat(processing_date)
    commits = []
    for dev_name, dev_email in DEVELOPERS:
        if _active_on_date(dt, repo_name, dev_email):
            n = _commits_on_date(dt, repo_name, dev_email)
            for i in range(n):
                c = _build_commit(dt, repo_name, dev_name, dev_email, i)
                c["organization"] = org
                commits.append(c)
    return commits


# ── Pull Request mock data ────────────────────────────────────────────────────

_PR_TITLES = [
    "Add user authentication flow",
    "Fix null pointer in API handler",
    "Refactor database connection pooling",
    "Implement data validation layer",
    "Add unit tests for payment module",
    "Optimize query performance",
    "Remove deprecated endpoints",
    "Update dependencies",
    "Add structured logging",
    "Fix memory leak in worker",
    "Implement retry logic for external API calls",
    "Refactor authentication middleware",
    "Add integration tests",
    "Improve error messages",
    "Add pagination to listing endpoints",
    "Implement two-factor authentication",
    "Database index optimization",
    "Add CSV export feature",
]

_BASE_REFS = ["main", "main", "main", "main", "develop"]
_HEAD_PREFIXES = ["feature/", "fix/", "chore/", "refactor/", "hotfix/"]


def _pr_id(dt: date, repo: str, org: str, idx: int) -> str:
    return hashlib.sha1(f"pr-{dt.isoformat()}-{org}/{repo}-{idx}".encode()).hexdigest()[:12]


def _pr_number(dt: date, repo: str, org: str, idx: int) -> int:
    base = _h(f"prbase-{org}/{repo}", 800) + 200
    daily = _h(f"prdaily-{dt.isoformat()}-{repo}", 5)
    return base + daily + idx


def get_pull_requests_for_repo_and_date(repo_name: str, org: str, processing_date: str) -> list[dict]:
    """Return pull requests created or updated on processing_date for the given repo.

    Simulates the GitHub REST API:
      GET /repos/{owner}/{repo}/pulls?state=all&since={processing_date}T00:00:00Z

    Returns 2-5 PRs deterministically per (repo, date).
    This is the integration point — replace with a real GitHub API call to go live.
    """
    dt = date.fromisoformat(processing_date)
    count = _h(f"prcount-{org}/{repo_name}-{dt.isoformat()}", 4) + 2  # 2-5 PRs

    prs = []
    for i in range(count):
        pr_id = _pr_id(dt, repo_name, org, i)
        number = _pr_number(dt, repo_name, org, i)
        seed = f"pr-{pr_id}"

        # Created at: between 07:00 and 14:00 on the processing_date
        created_hour = _h(f"prcreated_h-{seed}", 8) + 7
        created_min = _h(f"prcreated_m-{seed}", 60)
        created_at = datetime(dt.year, dt.month, dt.day, created_hour, created_min, 0)

        # ~75% of PRs get reviewed on the same day
        is_reviewed = _h(f"reviewed-{seed}", 4) < 3
        first_review_at = None
        first_reviewer = None
        if is_reviewed:
            review_hours_after = _h(f"reviewlag-{seed}", 6) + 1  # 1-6 hours after creation
            review_at = datetime(
                dt.year, dt.month, dt.day, min(created_hour + review_hours_after, 22), _h(f"reviewmin-{seed}", 60), 0
            )
            first_review_at = review_at.isoformat()
            dev_idx = _h(f"reviewer-{seed}", len(DEVELOPERS))
            first_reviewer = DEVELOPERS[dev_idx][0]

        # ~65% of reviewed PRs are merged on the same day
        is_merged = is_reviewed and _h(f"merged-{seed}", 10) < 7
        merged_at = None
        closed_at = None
        if is_merged and first_review_at:
            merge_min_after = _h(f"mergelag-{seed}", 120) + 15  # 15-135 min after review
            review_dt = datetime.fromisoformat(first_review_at)
            total_minutes = review_dt.hour * 60 + review_dt.minute + merge_min_after
            merge_hour = min(total_minutes // 60, 23)
            merge_minute = total_minutes % 60
            merge_ts = datetime(dt.year, dt.month, dt.day, merge_hour, merge_minute, 0)
            merged_at = merge_ts.isoformat()
            closed_at = merged_at

        state = "closed" if merged_at else "open"
        author_idx = _h(f"prauthor-{seed}", len(DEVELOPERS))
        author_name, author_email = DEVELOPERS[author_idx]
        head_prefix = _HEAD_PREFIXES[_h(f"headprefix-{seed}", len(_HEAD_PREFIXES))]
        title = _PR_TITLES[_h(f"prtitle-{seed}", len(_PR_TITLES))]

        prs.append(
            {
                "pr_id": pr_id,
                "number": number,
                "title": title,
                "state": state,
                "repository": repo_name,
                "organization": org,
                "author": author_name,
                "author_email": author_email,
                "created_at": created_at.isoformat(),
                "updated_at": (merged_at or first_review_at or created_at.isoformat()),
                "merged_at": merged_at,
                "closed_at": closed_at,
                "first_review_submitted_at": first_review_at,
                "first_reviewer": first_reviewer,
                "base_ref": _BASE_REFS[_h(f"baseref-{seed}", len(_BASE_REFS))],
                "head_ref": f"{head_prefix}{title.lower().replace(' ', '-')[:20]}",
                "additions": _h(f"pradd-{seed}", 400) + 5,
                "deletions": _h(f"prdel-{seed}", 200),
                "changed_files": _h(f"prfiles-{seed}", 15) + 1,
                "is_merged": is_merged,
                "draft": _h(f"draft-{seed}", 10) == 0,  # ~10% drafts
            }
        )

    return prs


# ── Deployment mock data ──────────────────────────────────────────────────────

_DEPLOYMENT_ENVS = ["staging", "staging", "staging", "production", "production"]
_DEPLOYMENT_STATUSES = ["success", "success", "success", "success", "failure"]
_DEPLOYMENT_DESCRIPTIONS = [
    "Automated deploy from CI pipeline",
    "Hotfix release",
    "Scheduled nightly deployment",
    "Feature release v2",
    "Rollback to stable",
]


def _deployment_id(dt: date, repo: str, org: str, idx: int) -> str:
    return hashlib.sha1(f"deploy-{dt.isoformat()}-{org}/{repo}-{idx}".encode()).hexdigest()[:12]


def get_deployments_for_repo_and_date(repo_name: str, org: str, processing_date: str) -> list[dict]:
    """Return deployments created on processing_date for the given repo.

    Simulates the GitHub REST API:
      GET /repos/{owner}/{repo}/deployments?created_since={processing_date}T00:00:00Z
      + deployment status from GET /repos/{owner}/{repo}/deployments/{deployment_id}/statuses

    Returns 1-3 deployments deterministically per (repo, date).
    This is the integration point — replace with a real GitHub API call to go live.
    """
    dt = date.fromisoformat(processing_date)
    # Only deploy on weekdays with some probability
    if dt.weekday() >= 5:
        if _h(f"wknd-deploy-{org}/{repo_name}-{dt.isoformat()}", 5) == 0:
            return []  # ~80% skip weekends
    count = _h(f"deploycount-{org}/{repo_name}-{dt.isoformat()}", 3) + 1  # 1-3

    deployments = []
    for i in range(count):
        deploy_id = _deployment_id(dt, repo_name, org, i)
        seed = f"deploy-{deploy_id}"

        deploy_hour = _h(f"dh-{seed}", 8) + 10  # 10:00 - 17:00
        deploy_min = _h(f"dm-{seed}", 60)
        created_at = datetime(dt.year, dt.month, dt.day, deploy_hour, deploy_min, 0)

        duration_secs = _h(f"dur-{seed}", 480) + 60  # 60-540 seconds
        updated_at = created_at + timedelta(seconds=duration_secs)

        environment = _DEPLOYMENT_ENVS[_h(f"env-{seed}", len(_DEPLOYMENT_ENVS))]
        status = _DEPLOYMENT_STATUSES[_h(f"status-{seed}", len(_DEPLOYMENT_STATUSES))]
        creator_idx = _h(f"creator-{seed}", len(DEVELOPERS))

        deployments.append(
            {
                "deployment_id": deploy_id,
                "repository": repo_name,
                "organization": org,
                "environment": environment,
                "ref": _BRANCHES[_h(f"dref-{seed}", len(_BRANCHES))],
                "sha": hashlib.sha1(f"sha-{seed}".encode()).hexdigest(),
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
                "status": status,
                "creator": DEVELOPERS[creator_idx][0],
                "creator_email": DEVELOPERS[creator_idx][1],
                "description": _DEPLOYMENT_DESCRIPTIONS[_h(f"ddesc-{seed}", len(_DEPLOYMENT_DESCRIPTIONS))],
                "duration_seconds": duration_secs,
            }
        )

    return deployments
