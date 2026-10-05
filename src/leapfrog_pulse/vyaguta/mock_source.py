"""Deterministic mock Vyaguta data source.

This module simulates the Vyaguta internal organization API. It is the only
component that needs to be replaced when integrating the real Vyaguta API.
All downstream Bronze/Silver/Gold logic is source-agnostic.

Vyaguta is Leapfrog Technology's internal HR/org tool. This mock covers
project metadata and project-member assignments. Project records include
associated GitHub repositories and Jira project keys; member records include
employee names and email addresses for downstream calendar ingestion.

Replacing this module:
    Implement get_all_projects() and get_project_members() by calling the real
    Vyaguta REST API and reshaping the responses to match RECORD_SCHEMA and
    MEMBER_SCHEMA, respectively.
"""

import hashlib

COMPANY = "leapfrog"

# fmt: off
_PROJECTS = [
    {
        "project_id":   "proj-001",
        "project_name": "HealthTrack Platform",
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
        "status":       "active",
        "github_repos": [
            "https://github.com/leapfrogonline/leapfrog_pulse",
        ],
        "jira_project_key": "DEVP",
    },
    {
        "project_id":   "proj-006",
        "project_name": "RetailPro POS",
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
                "status": project["status"],
                "github_repo_url": repo_url,
                "jira_project_key": project["jira_project_key"],
                "company": COMPANY,
                "source_hash": _source_hash(project["project_id"], repo_url),
            }
        )
    return rows


_PROJECT_MEMBERS = [
    {
        "project_id": "proj-001",
        "employee_name": "Aryan Sharma",
        "employee_email": "aryan.sharma@lftechnology.com",
        "role": "Backend Engineer",
    },
    {
        "project_id": "proj-001",
        "employee_name": "Priya Patel",
        "employee_email": "priya.patel@lftechnology.com",
        "role": "Frontend Engineer",
    },
    {
        "project_id": "proj-002",
        "employee_name": "Bikash Thapa",
        "employee_email": "bikash.thapa@lftechnology.com",
        "role": "Full Stack Engineer",
    },
    {
        "project_id": "proj-002",
        "employee_name": "Sanjana Rai",
        "employee_email": "sanjana.rai@lftechnology.com",
        "role": "Backend Engineer",
    },
    {
        "project_id": "proj-003",
        "employee_name": "Diwas Gurung",
        "employee_email": "diwas.gurung@lftechnology.com",
        "role": "Full Stack Engineer",
    },
    {
        "project_id": "proj-003",
        "employee_name": "Aryan Sharma",
        "employee_email": "aryan.sharma@lftechnology.com",
        "role": "Tech Lead",
    },
    {
        "project_id": "proj-004",
        "employee_name": "Priya Patel",
        "employee_email": "priya.patel@lftechnology.com",
        "role": "Backend Engineer",
    },
    {
        "project_id": "proj-004",
        "employee_name": "Bikash Thapa",
        "employee_email": "bikash.thapa@lftechnology.com",
        "role": "DevOps Engineer",
    },
    {
        "project_id": "proj-005",
        "employee_name": "Sanjana Rai",
        "employee_email": "sanjana.rai@lftechnology.com",
        "role": "Data Engineer",
    },
    {
        "project_id": "proj-005",
        "employee_name": "Diwas Gurung",
        "employee_email": "diwas.gurung@lftechnology.com",
        "role": "Backend Engineer",
    },
]

MEMBER_SCHEMA = ["project_id", "employee_name", "employee_email", "role", "company"]


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


def get_project_members() -> list[dict]:
    """Return all project members with project assignments.

    Each record represents one (employee, project) pair. Output is fully
    deterministic. This is the integration point — replace with a real Vyaguta
    API call to go live.
    """
    rows = []
    for member in _PROJECT_MEMBERS:
        rows.append({**member, "company": COMPANY})
    return rows


def get_unique_engineers() -> list[dict]:
    """Return deduplicated employees regardless of project.

    Removes duplicates by email — each engineer appears once. Useful for
    computing org-wide eligible working hours.
    """
    seen_emails = set()
    rows = []
    for member in _PROJECT_MEMBERS:
        if member["employee_email"] not in seen_emails:
            rows.append({**member, "company": COMPANY})
            seen_emails.add(member["employee_email"])
    return rows
