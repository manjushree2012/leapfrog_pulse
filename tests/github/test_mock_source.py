"""Tests for mock_source.py — pure Python, no Spark required."""

from leapfrog_pulse.github.mock_source import (
    DATA_END_DATE,
    DATA_START_DATE,
    DEVELOPERS,
    ORGANIZATION,
    REPOSITORIES,
    get_all_commits,
    get_commits_for_date,
)


class TestGetCommitsForDate:
    def test_returns_list_of_dicts(self):
        commits = get_commits_for_date("2026-09-15")
        assert isinstance(commits, list)
        assert all(isinstance(c, dict) for c in commits)

    def test_deterministic_same_date(self):
        a = get_commits_for_date("2026-09-15")
        b = get_commits_for_date("2026-09-15")
        assert a == b

    def test_deterministic_different_dates_differ(self):
        a = get_commits_for_date("2026-09-10")
        b = get_commits_for_date("2026-09-11")
        ids_a = {c["commit_id"] for c in a}
        ids_b = {c["commit_id"] for c in b}
        assert ids_a.isdisjoint(ids_b), "Different dates must produce different commit IDs"

    def test_commit_ids_unique_within_date(self):
        commits = get_commits_for_date("2026-09-15")
        ids = [c["commit_id"] for c in commits]
        assert len(ids) == len(set(ids)), "commit_id must be unique within a date"

    def test_only_target_date_commits_returned(self):
        commits = get_commits_for_date("2026-09-20")
        for c in commits:
            assert c["committed_at"].startswith("2026-09-20"), (
                f"Expected committed_at on 2026-09-20, got {c['committed_at']}"
            )

    def test_multiple_repositories_present(self):
        commits = get_commits_for_date("2026-09-15")
        repos = {c["repository"] for c in commits}
        assert len(repos) >= 2, f"Expected multiple repos, got: {repos}"
        assert repos.issubset(set(REPOSITORIES))

    def test_multiple_developers_present(self):
        commits = get_commits_for_date("2026-09-15")
        authors = {c["author_email"] for c in commits}
        assert len(authors) >= 2, f"Expected multiple developers, got: {authors}"

    def test_required_fields_present(self):
        required = {
            "commit_id", "organization", "repository", "author",
            "author_email", "committed_at", "message", "branch",
            "additions", "deletions", "changed_files",
        }
        commits = get_commits_for_date("2026-09-15")
        assert len(commits) > 0
        for c in commits:
            missing = required - c.keys()
            assert not missing, f"Commit missing fields: {missing}"

    def test_organization_is_leapfrog(self):
        commits = get_commits_for_date("2026-09-15")
        for c in commits:
            assert c["organization"] == ORGANIZATION

    def test_numeric_fields_are_non_negative(self):
        commits = get_commits_for_date("2026-09-15")
        for c in commits:
            assert c["additions"] >= 0, f"additions negative: {c}"
            assert c["deletions"] >= 0, f"deletions negative: {c}"
            assert c["changed_files"] >= 1, f"changed_files must be >= 1: {c}"

    def test_empty_for_non_data_range_date(self):
        commits = get_commits_for_date("2020-01-01")
        assert commits == [], "Dates outside mock range should return no commits"

    def test_all_repos_appear_over_range(self):
        all_commits = get_all_commits()
        repos = {c["repository"] for c in all_commits}
        assert repos == set(REPOSITORIES), f"All repos expected, got: {repos}"

    def test_all_developers_appear_over_range(self):
        all_commits = get_all_commits()
        emails = {c["author_email"] for c in all_commits}
        expected_emails = {email for _, email in DEVELOPERS}
        assert emails == expected_emails, f"All devs expected, got: {emails}"

    def test_data_range_spans_30_days(self):
        days = (DATA_END_DATE - DATA_START_DATE).days + 1
        assert days == 30, f"Expected 30 days, got {days}"

    def test_get_all_commits_deterministic(self):
        a = get_all_commits()
        b = get_all_commits()
        assert a == b
