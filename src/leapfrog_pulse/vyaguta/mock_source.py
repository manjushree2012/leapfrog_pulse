"""Deterministic mock Vyaguta data source.

This module simulates the Vyaguta internal organization API. It is the only
component that needs to be replaced when integrating the real Vyaguta API.
All downstream Bronze/Silver/Gold logic is source-agnostic.

Vyaguta is Leapfrog Technology's internal HR/org tool. This mock covers the
Projects domain: each project exposes its associated GitHub repositories and
Jira project keys. This data is relatively static and is loaded once (not on
a daily schedule) so that downstream pipelines can look up repo/project metadata.

Replacing this module:
    Implement get_all_projects() -> list[dict] by calling the real Vyaguta REST
    API and reshaping the response to match the field schema defined below.
    The returned dicts must match the same field schema defined in RECORD_SCHEMA.
"""

import hashlib

COMPANY = "leapfrog"

# fmt: off
_PROJECTS = [
    {
        "project_id":   "proj-001",
        "project_name": "HealthTrack Platform",
        "team":         "HealthTech",
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/healthtrack-api",
            "https://github.com/leapfrogonline/healthtrack-web",
        ],
        "jira_project_key": "HTP",
    },
    {
        "project_id":   "proj-002",
        "project_name": "FinEdge Analytics",
        "team":         "FinTech",
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/finedge-analytics",
            "https://github.com/leapfrogonline/finedge-mobile",
        ],
        "jira_project_key": "FEA",
    },
    {
        "project_id":   "proj-003",
        "project_name": "EduConnect LMS",
        "team":         "EdTech",
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/educonnect-backend",
            "https://github.com/leapfrogonline/educonnect-frontend",
            "https://github.com/leapfrogonline/educonnect-mobile",
        ],
        "jira_project_key": "ECL",
    },
    {
        "project_id":   "proj-004",
        "project_name": "LogiTrack Supply Chain",
        "team":         "Enterprise",
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/logitrack-core",
            "https://github.com/leapfrogonline/logitrack-dashboard",
        ],
        "jira_project_key": "LSC",
    },
    {
        "project_id":   "proj-005",
        "project_name": "DevPulse Internal",
        "team":         "Engineering Intelligence",
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/leapfrog_pulse",
        ],
        "jira_project_key": "DEVP",
    },
    {
        "project_id":   "proj-006",
        "project_name": "RetailPro POS",
        "team":         "Retail",
        "status":       "archived",
        "github_repos": [
            "https://github.com/leapfrogonline/retailpro-pos",
        ],
        "jira_project_key": "RPP",
    },
]
# fmt: on

# The schema every record returned by this module conforms to.
# Keys map 1-to-1 to the bronze table columns.
RECORD_SCHEMA = [
    "project_id",  # str  — stable unique identifier
    "project_name",  # str
    "team",  # str  — internal team name
    "status",  # str  — "active" | "archived"
    "github_repo_url",  # str  — one row per repo (exploded from github_repos)
    "jira_project_key",  # str  — Jira project key; used to look up issues later
    "company",  # str  — always COMPANY
    "source_hash",  # str  — SHA-256 of (project_id + github_repo_url) for dedup
]


def _source_hash(project_id: str, github_repo_url: str) -> str:
    """Stable content hash used as the primary key for deduplication."""
    return hashlib.sha256(f"{project_id}::{github_repo_url}".encode()).hexdigest()


def _explode_project(project: dict) -> list[dict]:
    """Expand one project record into one row per GitHub repository."""
    rows = []
    for repo_url in project["github_repos"]:
        rows.append(
            {
                "project_id": project["project_id"],
                "project_name": project["project_name"],
                "team": project["team"],
                "status": project["status"],
                "github_repo_url": repo_url,
                "jira_project_key": project["jira_project_key"],
                "company": COMPANY,
                "source_hash": _source_hash(project["project_id"], repo_url),
            }
        )
    return rows


_TEAM_MEMBERS = [
    # HealthTech
    {
        "project_id": "proj-001",
        "employee_name": "Aryan Sharma",
        "employee_email": "aryan.sharma@lftechnology.com",
        "role": "Backend Engineer",
        "team": "HealthTech",
    },
    {
        "project_id": "proj-001",
        "employee_name": "Priya Patel",
        "employee_email": "priya.patel@lftechnology.com",
        "role": "Frontend Engineer",
        "team": "HealthTech",
    },
    # FinTech
    {
        "project_id": "proj-002",
        "employee_name": "Bikash Thapa",
        "employee_email": "bikash.thapa@lftechnology.com",
        "role": "Full Stack Engineer",
        "team": "FinTech",
    },
    {
        "project_id": "proj-002",
        "employee_name": "Sanjana Rai",
        "employee_email": "sanjana.rai@lftechnology.com",
        "role": "Backend Engineer",
        "team": "FinTech",
    },
    # EdTech
    {
        "project_id": "proj-003",
        "employee_name": "Diwas Gurung",
        "employee_email": "diwas.gurung@lftechnology.com",
        "role": "Full Stack Engineer",
        "team": "EdTech",
    },
    {
        "project_id": "proj-003",
        "employee_name": "Aryan Sharma",
        "employee_email": "aryan.sharma@lftechnology.com",
        "role": "Tech Lead",
        "team": "EdTech",
    },
    # Enterprise
    {
        "project_id": "proj-004",
        "employee_name": "Priya Patel",
        "employee_email": "priya.patel@lftechnology.com",
        "role": "Backend Engineer",
        "team": "Enterprise",
    },
    {
        "project_id": "proj-004",
        "employee_name": "Bikash Thapa",
        "employee_email": "bikash.thapa@lftechnology.com",
        "role": "DevOps Engineer",
        "team": "Enterprise",
    },
    # Engineering Intelligence
    {
        "project_id": "proj-005",
        "employee_name": "Sanjana Rai",
        "employee_email": "sanjana.rai@lftechnology.com",
        "role": "Data Engineer",
        "team": "Engineering Intelligence",
    },
    {
        "project_id": "proj-005",
        "employee_name": "Diwas Gurung",
        "employee_email": "diwas.gurung@lftechnology.com",
        "role": "Backend Engineer",
        "team": "Engineering Intelligence",
    },
]

MEMBER_SCHEMA = ["project_id", "employee_name", "employee_email", "role", "team", "company"]


def get_all_projects() -> list[dict]:
    """Return all project-repository mappings known to Vyaguta.

    Each record represents one (project, github_repo) pair so that downstream
    consumers can join directly on github_repo_url without further exploding.
    Output is fully deterministic. This is the integration point — replace with
    a real Vyaguta API call to go live.
    """
    rows: list[dict] = []
    for project in _PROJECTS:
        rows.extend(_explode_project(project))
    return rows


def get_team_members() -> list[dict]:
    """Return all team members with project assignments.

    Each record represents one (employee, project) pair. Output is fully
    deterministic. This is the integration point — replace with a real Vyaguta
    API call to go live.
    """
    rows = []
    for member in _TEAM_MEMBERS:
        rows.append({**member, "company": COMPANY})
    return rows


def get_unique_engineers() -> list[dict]:
    """Return deduplicated employees regardless of project.

    Removes duplicates by email — each engineer appears once. Useful for
    computing org-wide eligible working hours.
    """
    seen_emails = set()
    rows = []
    for member in _TEAM_MEMBERS:
        if member["employee_email"] not in seen_emails:
            rows.append({**member, "company": COMPANY})
            seen_emails.add(member["employee_email"])
    return rows
