"""Deterministic mock Google Calendar data source.

This module simulates the Google Calendar API. It is the only component that
needs to be replaced when integrating the real Google Calendar API.

Authentication for the real API:
    Use google-auth with OAuth 2.0 service account credentials or user OAuth flow
    (google.oauth2.service_account.Credentials or google_auth_oauthlib.flow).
    Scopes required: https://www.googleapis.com/auth/calendar.readonly
    Use the googleapiclient.discovery.build('calendar','v3') client.

Replacing this module:
    Implement get_events_for_date(processing_date: str, engineer_emails:
    list[str]) -> list[dict] using the real Google Calendar API to fetch each
    supplied team member's calendar. The returned dicts must match the field
    schema defined in RECORD_SCHEMA.

Event eligibility rules (configurable, applied before returning):
    - status != "cancelled"
    - duration_hours > 0
    - at least one accepted attendee who is an engineering team member
    - event title does not match EXCLUDED_TITLE_PATTERNS
"""

import hashlib
from datetime import date, datetime, timedelta

_MEETING_TEMPLATES = [
    {"title": "Daily Standup", "category": "standup", "duration_minutes": 15, "min_attendees": 3, "max_attendees": 5},
    {"title": "Sprint Planning", "category": "planning", "duration_minutes": 60, "min_attendees": 4, "max_attendees": 5},
    {"title": "Retrospective", "category": "retrospective", "duration_minutes": 90, "min_attendees": 4, "max_attendees": 5},
    {"title": "1-on-1", "category": "1on1", "duration_minutes": 30, "min_attendees": 2, "max_attendees": 2},
    {"title": "Code Review Meeting", "category": "code_review_meeting", "duration_minutes": 60, "min_attendees": 3, "max_attendees": 4},
    {"title": "Backlog Grooming", "category": "planning", "duration_minutes": 45, "min_attendees": 3, "max_attendees": 5},
]

DATA_START_DATE = date(2026, 9, 1)
DATA_END_DATE = date(2026, 9, 30)

# The schema every record returned by this module conforms to.
RECORD_SCHEMA = [
    "event_id",                # str
    "title",                   # str
    "start_at",                # str ISO timestamp
    "end_at",                  # str ISO timestamp
    "duration_hours",          # float
    "organizer_email",         # str
    "attendee_emails",         # str — JSON-encoded list
    "attendee_count",          # int
    "accepted_count",          # int — attendees who accepted
    "status",                  # str — "confirmed" | "cancelled"
    "is_recurring",            # bool
    "category",                # str
    "is_engineering_meeting",  # bool
    "attendee_hours",          # float — duration_hours * accepted_count
]


def _h(key: str, modulo: int) -> int:
    """Deterministic hash function."""
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % modulo


def _event_id(template_idx: int, dt: date, event_idx: int) -> str:
    """Generate a plausible event ID."""
    base = _h(f"event-base-{dt.isoformat()}-{template_idx}", 1000) + 10000
    offset = _h(f"event-offset-{dt.isoformat()}-{template_idx}-{event_idx}", 100)
    return f"evt_{base + offset}"


def _build_event(template_idx: int, dt: date, event_idx: int, engineer_emails: list[str]) -> dict:
    """Generate a single mock event."""
    template = _MEETING_TEMPLATES[template_idx]
    event_id = _event_id(template_idx, dt, event_idx)
    seed = f"{event_id}-{dt.isoformat()}"

    # Duration is always from template
    duration_minutes = template["duration_minutes"]
    duration_hours = duration_minutes / 60.0

    # Time of day
    hour = _h(f"hr-{seed}", 9) + 8  # 08:00–16:00
    minute = _h(f"mn-{seed}", 60)
    start_at = datetime(dt.year, dt.month, dt.day, hour, minute, 0)
    end_at = start_at + timedelta(minutes=duration_minutes)

    # Attendees: deterministic subset of the Vyaguta-supplied team members
    attendee_count = min(
        len(engineer_emails),
        _h(f"att-count-{seed}", template["max_attendees"] - template["min_attendees"] + 1)
        + template["min_attendees"],
    )
    attendees = sorted(engineer_emails, key=lambda email: (_h(f"att-{seed}-{email}", len(engineer_emails)), email))
    attendee_emails = attendees[:attendee_count]

    # Acceptance rate: ~80% accept
    accepted_count = 0
    for email in attendee_emails:
        if _h(f"accept-{seed}-{email}", 100) < 80:
            accepted_count += 1

    # Status: ~85% confirmed, ~15% cancelled
    is_cancelled = _h(f"status-{seed}", 100) >= 85
    status = "cancelled" if is_cancelled else "confirmed"
    if status == "confirmed" and attendee_emails and accepted_count == 0:
        accepted_count = 1

    # Organizer is a random engineer
    organizer_email = engineer_emails[_h(f"org-{seed}", len(engineer_emails))]

    # Is recurring
    is_recurring = _h(f"recur-{seed}", 100) < 20  # ~20% recurring

    # Category from template
    category = template["category"]

    # attendee_hours: duration_hours * accepted_count (0 if cancelled)
    if status == "cancelled":
        attendee_hours = 0.0
    else:
        attendee_hours = duration_hours * accepted_count

    # is_engineering_meeting: True when category != "other" and status == "confirmed" and at least 2 engineers
    is_engineering_meeting = (
        category != "other" and status == "confirmed" and len(attendee_emails) >= 2
    )

    import json

    return {
        "event_id": event_id,
        "title": template["title"],
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "duration_hours": round(duration_hours, 2),
        "organizer_email": organizer_email,
        "attendee_emails": json.dumps(attendee_emails),
        "attendee_count": len(attendee_emails),
        "accepted_count": accepted_count,
        "status": status,
        "is_recurring": is_recurring,
        "category": category,
        "is_engineering_meeting": is_engineering_meeting,
        "attendee_hours": round(attendee_hours, 2),
    }


def get_events_for_date(processing_date: str, engineer_emails: list[str]) -> list[dict]:
    """Return Google Calendar events on processing_date.

    Generates events only for the supplied team-member calendars. Returns []
    when there are no team members or the date is a weekend.
    Output is fully deterministic. This is the integration point — replace with
    a real Google Calendar API call to go live.
    """
    dt = date.fromisoformat(processing_date)
    engineer_emails = sorted({email.strip().lower() for email in engineer_emails if email and email.strip()})

    # No events on weekends (Monday=0, Sunday=6)
    if dt.weekday() >= 5 or not engineer_emails:
        return []

    events = []
    event_count = _h(f"events-{processing_date}", 3) + 2  # 2-4 events per day
    for i in range(event_count):
        template_idx = _h(f"template-{processing_date}-{i}", len(_MEETING_TEMPLATES))
        event = _build_event(template_idx, dt, i, engineer_emails)
        events.append(event)

    return events


def get_events_for_date_range(start_date: str, end_date: str, engineer_emails: list[str]) -> list[dict]:
    """Return Google Calendar events for a date range."""
    events = []
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    current = start
    while current <= end:
        events.extend(get_events_for_date(current.isoformat(), engineer_emails))
        current += timedelta(days=1)
    return events
