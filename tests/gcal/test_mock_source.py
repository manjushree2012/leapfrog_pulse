import json

from leapfrog_pulse.gcal.mock_source import get_events_for_date


def test_events_use_only_supplied_team_member_emails():
    engineer_emails = ["member-b@example.com", "member-a@example.com"]

    events = get_events_for_date("2026-10-05", engineer_emails)

    assert events
    for event in events:
        assert event["organizer_email"] in engineer_emails
        assert set(json.loads(event["attendee_emails"])) <= set(engineer_emails)


def test_events_are_empty_when_there_are_no_team_members():
    assert get_events_for_date("2026-10-05", []) == []
