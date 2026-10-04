"""Deterministic mock JIRA data source.

Simulates fetching issues updated within a given day per project. This mirrors
the JIRA REST API query: project = <KEY> AND updated >= <date> AND updated < <date+1day>.

Replacing this module:
    Implement get_issues_for_project_and_date(project_key, processing_date) -> list[dict]
    using the real JIRA REST API. The returned dicts must match the same field schema
    defined in RECORD_SCHEMA.
"""

import hashlib
from datetime import date, datetime, timedelta

_STATUSES = ["To Do", "In Progress", "In Review", "Done"]
_ISSUE_TYPES = ["Story", "Story", "Story", "Story", "Story", "Story", "Story", "Bug", "Bug", "Task", "Sub-task"]
_PRIORITIES = ["Critical", "High", "Medium", "Low"]
_STORY_POINTS = [1, 2, 3, 5, 8, 13]

_DEVELOPERS = [
    "Aryan Sharma",
    "Priya Patel",
    "Bikash Thapa",
    "Sanjana Rai",
    "Diwas Gurung",
]

_SUMMARIES = [
    "Implement user dashboard",
    "Fix login redirect loop",
    "Add pagination to search results",
    "Refactor authentication service",
    "Write API documentation",
    "Upgrade third-party dependencies",
    "Resolve intermittent test failures",
    "Improve error handling in payment flow",
    "Add audit logging for sensitive operations",
    "Performance optimization for report generation",
    "Implement email notification service",
    "Fix date formatting in export",
    "Add dark mode support",
    "Database index optimization",
    "Address security vulnerability in file upload",
    "Implement two-factor authentication",
    "Add CSV export for data tables",
    "Fix broken links in onboarding flow",
    "Improve mobile responsiveness",
    "Cache frequently accessed config values",
]

_SPRINT_BASE = 40

# The schema every record returned by this module conforms to.
RECORD_SCHEMA = [
    "issue_key",     # str  — e.g. "HTP-123"
    "project_key",   # str  — e.g. "HTP"
    "summary",       # str
    "status",        # str  — "To Do" | "In Progress" | "In Review" | "Done"
    "issue_type",    # str  — "Story" | "Bug" | "Task" | "Sub-task"
    "priority",      # str  — "Critical" | "High" | "Medium" | "Low"
    "assignee",      # str  — developer name
    "reporter",      # str  — developer name
    "created_at",    # str  ISO timestamp
    "updated_at",    # str  ISO timestamp — within the processing date
    "resolved_at",   # str | None  ISO timestamp — set for resolved issues
    "story_points",  # int
    "sprint_name",   # str
    "worklog_hours",   # float — total hours logged on this issue on this date (0.0 if none)
    "has_worklogs",    # bool  — True when actual worklog data is available
]


def _h(key: str, modulo: int) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % modulo


def _issue_number(project_key: str, date_str: str, idx: int) -> int:
    """Generate a plausible ever-growing issue number."""
    base = _h(f"base-{project_key}", 500) + 100
    daily_offset = _h(f"daily-{project_key}-{date_str}", 30)
    return base + daily_offset + idx


def _build_issue(project_key: str, dt: date, idx: int) -> dict:
    number = _issue_number(project_key, dt.isoformat(), idx)
    issue_key = f"{project_key}-{number}"
    seed = f"{issue_key}-{dt.isoformat()}"

    hour = _h(f"hr-{seed}", 9) + 8  # 08:00–16:00
    minute = _h(f"mn-{seed}", 60)
    updated_at = datetime(dt.year, dt.month, dt.day, hour, minute, 0)

    created_days_ago = _h(f"cdays-{seed}", 30) + 1
    created_date = dt - timedelta(days=created_days_ago)
    created_at = datetime(created_date.year, created_date.month, created_date.day, 9, 0, 0)

    sprint_num = _SPRINT_BASE + dt.isocalendar()[1]
    status = _STATUSES[_h(f"sta-{seed}", len(_STATUSES))]
    issue_type = _ISSUE_TYPES[_h(f"typ-{seed}", len(_ISSUE_TYPES))]
    priority = _PRIORITIES[_h(f"pri-{seed}", len(_PRIORITIES))]

    # Add occasional completed high-severity bugs as mock incident records.
    if idx == 0 and _h(f"incident-{project_key}-{dt.isoformat()}", 5) == 0:
        status = "Done"
        issue_type = "Bug"
        priority = "Critical"

    # Worklog hours: ~60% of issues have worklogs; when present, 0.5–7.0 hours.
    has_worklog = _h(f"wl-has-{seed}", 100) < 60
    if has_worklog:
        worklog_hours = float(_h(f"wl-{seed}", 14) * 0.5 + 0.5)
    else:
        worklog_hours = 0.0

    return {
        "issue_key": issue_key,
        "project_key": project_key,
        "summary": _SUMMARIES[_h(f"sum-{seed}", len(_SUMMARIES))],
        "status": status,
        "issue_type": issue_type,
        "priority": priority,
        "assignee": _DEVELOPERS[_h(f"asg-{seed}", len(_DEVELOPERS))],
        "reporter": _DEVELOPERS[_h(f"rep-{seed}", len(_DEVELOPERS))],
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "resolved_at": updated_at.isoformat() if status == "Done" else None,
        "story_points": _STORY_POINTS[_h(f"sp-{seed}", len(_STORY_POINTS))],
        "sprint_name": f"Sprint {sprint_num}",
        "worklog_hours": worklog_hours,
        "has_worklogs": has_worklog,
    }


def get_issues_for_project_and_date(project_key: str, processing_date: str) -> list[dict]:
    """Return JIRA issues updated on processing_date for the given project.

    Simulates: project = <project_key> AND updated >= <date> AND updated < <date+1day>
    Returns between 2 and 5 issues deterministically per (project_key, date).
    This is the integration point — replace with a real JIRA REST API call to go live.
    """
    dt = date.fromisoformat(processing_date)
    count = _h(f"count-{project_key}-{processing_date}", 4) + 2  # 2–5 issues
    return [_build_issue(project_key, dt, i) for i in range(count)]
