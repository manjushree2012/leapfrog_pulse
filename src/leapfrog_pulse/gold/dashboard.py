"""Dashboard Gold Mart — cross-source analytics tables for the DevPulse dashboard.

Joins Vyaguta Gold (project catalogue), GitHub Gold (commit metrics), and
JIRA Gold (issue metrics) into three analytics-ready tables consumed by the
Node.js dashboard API.

Tables produced:
  dashboard_gold_project_summary
      One row per active project. Rolling 30-day metrics + health score (0-100).
      Feeds: /api/projects, /api/teams, /api/vyaguta

  dashboard_gold_kpis
      One row per day. Org-wide KPI snapshot.
      Feeds: /api/kpis, /api/health

  dashboard_gold_recent_activity
      Top 50 most recent events (commits + JIRA updates).
      Feeds: /api/activity

Entry point: dashboard_gold
"""

import argparse
import calendar
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Constants for time allocation computation
CODE_REVIEW_HRS_PER_PR = 1.5   # estimated hours per PR review (no actual review time data)
HOURS_PER_WORKING_DAY = 8


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_date(processing_date: str) -> date:
    if processing_date == "yesterday":
        return date.today() - timedelta(days=1)
    return date.fromisoformat(processing_date)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# dashboard_gold_project_summary
# ---------------------------------------------------------------------------

def compute_project_summary(spark: SparkSession, catalog: str, schema: str, as_of: date) -> DataFrame:
    window_start = (as_of - timedelta(days=29)).isoformat()
    window_end = as_of.isoformat()

    # Vyaguta: active project catalogue (project_id, project_name, team, repo per row)
    vy = spark.table(f"`{catalog}`.`{schema}`.vyaguta_gold_project_catalogue")
    vy_projects = vy.groupBy("project_id", "project_name", "team").agg(
        F.count("github_repo_url").alias("repo_count"),
    )

    # GitHub: 30-day rolling commit metrics per project
    gh = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .groupBy("project_id")
        .agg(
            F.sum("commit_count").alias("commits_last_30d"),
            F.countDistinct("developer").alias("active_developers"),
            F.sum("total_additions").alias("lines_added_30d"),
            F.sum("total_deletions").alias("lines_removed_30d"),
        )
    )

    # Jira's resolved critical/high bugs provide the mock incident recovery proxy.
    # Use each issue's latest silver record to avoid counting daily snapshots twice.
    issue_window = Window.partitionBy("issue_key").orderBy(
        F.col("processing_date").desc(),
        F.col("updated_at").desc(),
    )
    jr_latest = (
        spark.table(f"`{catalog}`.`{schema}`.jira_silver_issues")
        .withColumn("_rn", F.row_number().over(issue_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .groupBy("project_id")
        .agg(
            F.sum(F.when(F.col("status") == "To Do", 1).otherwise(0)).alias("open_issues"),
            F.sum(F.when(F.col("status").isin("In Progress", "In Review"), 1).otherwise(0))
            .alias("in_progress_issues"),
            F.sum(F.when(F.col("status") == "Done", 1).otherwise(0)).alias("done_issues"),
            F.sum(F.when(F.col("is_bug"), 1).otherwise(0)).alias("bugs_count"),
            F.sum(F.when(F.col("issue_type") == "Story", 1).otherwise(0)).alias("stories_count"),
            F.sum(F.when(F.col("issue_type") == "Task", 1).otherwise(0)).alias("tasks_count"),
            F.round(F.avg("story_points"), 1).alias("avg_story_points"),
            F.max("sprint_name").alias("sprint_name"),
            F.sum(
                F.when(
                    F.col("is_bug")
                    & F.col("priority").isin("Critical", "High")
                    & F.col("created_at").isNotNull()
                    & F.col("resolved_at").isNotNull()
                    & (F.col("resolved_at") >= F.col("created_at")),
                    1,
                ).otherwise(0)
            ).alias("resolved_incidents"),
            F.round(
                F.avg(
                    F.when(
                        F.col("is_bug")
                        & F.col("priority").isin("Critical", "High")
                        & F.col("created_at").isNotNull()
                        & F.col("resolved_at").isNotNull()
                        & (F.col("resolved_at") >= F.col("created_at")),
                        (F.unix_timestamp("resolved_at") - F.unix_timestamp("created_at")) / 3600.0,
                    )
                ),
                2,
            ).alias("avg_incident_recovery_hrs"),
        )
    )

    # JIRA: 30-day done count (velocity proxy)
    jr_30d = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .groupBy("project_id")
        .agg(F.sum("done_issues").alias("done_issues_last_30d"))
    )

    # Review time from the daily PR mart, weighted by the number of reviewed PRs
    pr_30d = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_pr_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .groupBy("project_id")
        .agg(
            F.sum("reviewed_prs").alias("reviewed_prs_last_30d"),
            F.round(
                F.sum(F.col("avg_review_time_hrs") * F.col("reviewed_prs"))
                / F.greatest(F.sum("reviewed_prs"), F.lit(1)),
                2,
            ).alias("avg_pr_review_time_hrs"),
        )
    )

    # CI check outcomes are PR-level data; aggregate the latest silver records
    # directly so daily PR gold upserts cannot overwrite checks from other PRs.
    ci_30d = (
        spark.table(f"`{catalog}`.`{schema}`.github_silver_project_prs")
        .filter(F.to_date("updated_at").between(window_start, window_end))
        .groupBy("project_id")
        .agg(
            F.sum(F.coalesce(F.col("ci_checks_total"), F.lit(0))).alias("ci_checks_total"),
            F.sum(F.coalesce(F.col("ci_checks_failed"), F.lit(0))).alias("ci_checks_failed"),
        )
    )

    # Lead time is measured from PR creation to merge, for PRs merged in-window.
    pr_lead_30d = (
        spark.table(f"`{catalog}`.`{schema}`.github_silver_project_prs")
        .filter(
            F.col("merged_at").isNotNull()
            & F.to_date("merged_at").between(window_start, window_end)
            & F.col("created_at").isNotNull()
            & (F.col("merged_at") >= F.col("created_at"))
        )
        .groupBy("project_id")
        .agg(
            F.countDistinct("pr_id").alias("merged_prs_last_30d"),
            F.round(
                F.avg(
                    (F.unix_timestamp("merged_at") - F.unix_timestamp("created_at"))
                    / F.lit(86400.0)
                ),
                2,
            ).alias("avg_lead_time_days"),
        )
    )

    # Successful production deployments per day in the rolling window
    dep_30d = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_deployment_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .groupBy("project_id")
        .agg(
            F.round(F.sum("successful_prod_deployments") / F.lit(30.0), 2)
            .alias("deploy_frequency_30d"),
        )
    )

    # Join everything — Vyaguta is the master project list
    result = (
        vy_projects
        .join(gh, "project_id", "left")
        .join(jr_latest, "project_id", "left")
        .join(jr_30d, "project_id", "left")
        .join(pr_30d, "project_id", "left")
        .join(ci_30d, "project_id", "left")
        .join(pr_lead_30d, "project_id", "left")
        .join(dep_30d, "project_id", "left")
    )

    # Health score (0-100):
    #   commit_score    (0-40): ≥20 commits/30d = 40 pts
    #   resolution_score (0-40): done/(done+open) * 40
    #   bug_score       (0-20): <3 bugs = 20, each bug above 3 = -4 pts
    result = result.withColumn(
        "commit_score",
        F.when(F.col("commits_last_30d").isNull(), F.lit(0))
         .otherwise(F.least(F.lit(40), (F.col("commits_last_30d") / F.lit(20.0) * 40).cast("int"))),
    ).withColumn(
        "resolution_score",
        F.when(
            F.col("done_issues_last_30d").isNull() | F.col("open_issues").isNull(),
            F.lit(0),
        ).otherwise(
            F.when(
                (F.col("done_issues_last_30d") + F.col("open_issues")) == 0,
                F.lit(20),
            ).otherwise(
                (F.col("done_issues_last_30d").cast("double")
                 / (F.col("done_issues_last_30d") + F.col("open_issues")) * 40).cast("int"),
            ),
        ),
    ).withColumn(
        "bug_score",
        F.when(F.col("bugs_count").isNull(), F.lit(20))
         .otherwise(F.greatest(F.lit(0), F.lit(20) - F.col("bugs_count") * 4)),
    ).withColumn(
        "health_score",
        F.least(
            F.lit(100),
            F.greatest(F.lit(0), F.col("commit_score") + F.col("resolution_score") + F.col("bug_score")),
        ),
    ).drop("commit_score", "resolution_score", "bug_score")

    return result.withColumn("as_of_date", F.lit(as_of.isoformat()).cast("date")).withColumn(
        "updated_at", F.lit(_now()).cast("timestamp")
    )


def ensure_project_summary_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.dashboard_gold_project_summary (
            project_id          STRING  NOT NULL,
            project_name        STRING,
            team                STRING,
            repo_count          INT,
            commits_last_30d    LONG,
            active_developers   LONG,
            lines_added_30d     LONG,
            lines_removed_30d   LONG,
            open_issues         INT,
            in_progress_issues  INT,
            done_issues         INT,
            bugs_count          INT,
            stories_count       INT,
            tasks_count         INT,
            avg_story_points    DOUBLE,
            sprint_name         STRING,
            done_issues_last_30d LONG,
            avg_lead_time_days  DOUBLE,
            merged_prs_last_30d LONG,
            avg_pr_review_time_hrs DOUBLE,
            deploy_frequency_30d DOUBLE,
            ci_checks_total     LONG,
            ci_checks_failed    LONG,
            resolved_incidents  LONG,
            avg_incident_recovery_hrs DOUBLE,
            health_score        INT,
            as_of_date          DATE,
            updated_at          TIMESTAMP
        )
        USING DELTA
        COMMENT '30-day rolling project health summary for the DevPulse dashboard'
    """)

    table_name = f"`{catalog}`.`{schema}`.dashboard_gold_project_summary"
    existing_columns = {field.name.lower() for field in spark.table(table_name).schema.fields}
    new_columns = {
        "avg_lead_time_days": "DOUBLE",
        "merged_prs_last_30d": "BIGINT",
        "avg_pr_review_time_hrs": "DOUBLE",
        "deploy_frequency_30d": "DOUBLE",
        "ci_checks_total": "BIGINT",
        "ci_checks_failed": "BIGINT",
        "resolved_incidents": "BIGINT",
        "avg_incident_recovery_hrs": "DOUBLE",
    }
    missing_columns = [f"{name} {data_type}" for name, data_type in new_columns.items() if name not in existing_columns]
    if missing_columns:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(missing_columns)})")


def merge_project_summary(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df = df.drop("reviewed_prs_last_30d")
    df.createOrReplaceTempView("_dashboard_project_summary_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.dashboard_gold_project_summary AS tgt
        USING _dashboard_project_summary_staging AS src
        ON tgt.project_id = src.project_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# dashboard_gold_kpis
# ---------------------------------------------------------------------------

def compute_kpis(spark: SparkSession, catalog: str, schema: str, as_of: date) -> DataFrame:
    window_start = (as_of - timedelta(days=29)).isoformat()
    window_end = as_of.isoformat()

    # GitHub org-wide 30-day totals
    gh = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .agg(
            F.sum("commit_count").alias("total_commits_30d"),
            F.countDistinct("developer").alias("active_developers"),
            F.countDistinct("project_id").alias("active_projects"),
            F.countDistinct("repository").alias("active_repos"),
        )
    )

    # JIRA org-wide current issue state, using each issue's latest silver record
    issue_window = Window.partitionBy("issue_key").orderBy(
        F.col("processing_date").desc(),
        F.col("updated_at").desc(),
    )
    jr = (
        spark.table(f"`{catalog}`.`{schema}`.jira_silver_issues")
        .withColumn("_rn", F.row_number().over(issue_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .agg(
            F.sum(F.when(F.col("status") == "To Do", 1).otherwise(0)).alias("total_open_issues"),
            F.sum(F.when(F.col("status").isin("In Progress", "In Review"), 1).otherwise(0))
            .alias("total_in_progress"),
            F.sum(F.when(F.col("status") == "Done", 1).otherwise(0)).alias("total_done_issues"),
            F.sum(F.when(F.col("is_bug"), 1).otherwise(0)).alias("total_bugs"),
            F.sum(F.when(F.col("issue_type") == "Story", 1).otherwise(0)).alias("total_stories"),
            F.sum(F.when(F.col("issue_type") == "Task", 1).otherwise(0)).alias("total_tasks"),
        )
    )

    # JIRA 30-day velocity: done issue count + resolved story points
    jr_30d = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .agg(
            F.sum("done_issues").alias("done_issues_30d"),
            F.sum("resolved_story_points").alias("velocity_story_points_30d"),
        )
    )

    # PR metrics: weighted-average review time over 30 days
    pr = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_pr_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .agg(
            F.round(
                F.sum(F.col("avg_review_time_hrs") * F.col("reviewed_prs"))
                / F.greatest(F.sum("reviewed_prs"), F.lit(1)),
                1,
            ).alias("avg_pr_review_time_hrs"),
            F.sum("total_prs").alias("total_prs_30d"),
            F.sum("merged_prs").alias("merged_prs_30d"),
        )
    )

    # Deployment metrics: successful production deployments per day over 30 days
    dep = (
        spark.table(f"`{catalog}`.`{schema}`.github_gold_deployment_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .agg(
            F.round(F.sum("successful_prod_deployments") / F.lit(30.0), 2).alias("deploy_frequency_30d"),
            F.sum("total_deployments").alias("total_deployments_30d"),
            F.sum("successful_deployments").alias("successful_deployments_30d"),
        )
    )

    # Project summary health average
    ph = spark.table(f"`{catalog}`.`{schema}`.dashboard_gold_project_summary").agg(
        F.round(F.avg("health_score"), 0).cast("int").alias("org_health_score"),
        F.count("project_id").alias("tracked_projects"),
    )

    # Cross-join single-row aggregations to produce one KPI row
    kpis = gh.crossJoin(jr).crossJoin(jr_30d).crossJoin(pr).crossJoin(dep).crossJoin(ph)

    return kpis.withColumn("kpi_date", F.lit(as_of.isoformat()).cast("date")).withColumn(
        "updated_at", F.lit(_now()).cast("timestamp")
    )


def ensure_kpis_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.dashboard_gold_kpis (
            kpi_date                    DATE    NOT NULL,
            total_commits_30d           LONG,
            active_developers           LONG,
            active_projects             LONG,
            active_repos                LONG,
            total_open_issues           LONG,
            total_in_progress           LONG,
            total_done_issues           LONG,
            done_issues_30d             LONG,
            velocity_story_points_30d   LONG,
            total_bugs                  LONG,
            total_stories               LONG,
            total_tasks                 LONG,
            avg_pr_review_time_hrs      DOUBLE,
            total_prs_30d               LONG,
            merged_prs_30d              LONG,
            deploy_frequency_30d        DOUBLE,
            total_deployments_30d       LONG,
            successful_deployments_30d  LONG,
            org_health_score            INT,
            tracked_projects            LONG,
            updated_at                  TIMESTAMP
        )
        USING DELTA
        COMMENT 'Org-wide daily KPI snapshot for the DevPulse dashboard top tiles'
    """)


def merge_kpis(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_dashboard_kpis_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.dashboard_gold_kpis AS tgt
        USING _dashboard_kpis_staging AS src
        ON tgt.kpi_date = src.kpi_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# dashboard_gold_recent_activity
# ---------------------------------------------------------------------------

def compute_recent_activity(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    commits = (
        spark.table(f"`{catalog}`.`{schema}`.github_silver_project_commits")
        .orderBy(F.col("committed_at").desc())
        .limit(25)
        .select(
            F.concat_ws("-", F.lit("commit"), F.col("commit_id")).alias("event_id"),
            F.lit("commit").alias("event_type"),
            F.col("committed_at").alias("event_time"),
            F.col("project_id"),
            F.col("project_name"),
            F.col("repository"),
            F.col("author").alias("actor"),
            F.col("commit_message").alias("description"),
            F.lit("GitHub").alias("source"),
            F.lit(None).cast("string").alias("priority"),
            F.lit(None).cast("string").alias("status"),
        )
    )

    jira = (
        spark.table(f"`{catalog}`.`{schema}`.jira_silver_issues")
        .orderBy(F.col("updated_at").desc())
        .limit(25)
        .select(
            F.concat_ws("-", F.lit("jira"), F.col("issue_key")).alias("event_id"),
            F.lit("jira_update").alias("event_type"),
            F.col("updated_at").alias("event_time"),
            F.col("project_id"),
            F.col("project_name"),
            F.lit(None).cast("string").alias("repository"),
            F.coalesce(F.col("assignee"), F.col("reporter")).alias("actor"),
            F.concat_ws(" — ", F.col("issue_key"), F.col("summary")).alias("description"),
            F.lit("Jira").alias("source"),
            F.col("priority"),
            F.col("status"),
        )
    )

    return (
        commits.union(jira)
        .orderBy(F.col("event_time").desc())
        .limit(50)
        .withColumn("updated_at", F.lit(_now()).cast("timestamp"))
    )


def ensure_activity_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.dashboard_gold_recent_activity (
            event_id        STRING  NOT NULL,
            event_type      STRING,
            event_time      TIMESTAMP,
            project_id      STRING,
            project_name    STRING,
            repository      STRING,
            actor           STRING,
            description     STRING,
            source          STRING,
            priority        STRING,
            status          STRING,
            updated_at      TIMESTAMP
        )
        USING DELTA
        COMMENT 'Latest 50 cross-platform events (commits + JIRA updates) for the activity feed'
    """)


def refresh_activity(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    # Overwrite entirely — this is a top-N list, not an append table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.`{schema}`.dashboard_gold_recent_activity"
    )


# ---------------------------------------------------------------------------
# dashboard_gold_time_allocation
# ---------------------------------------------------------------------------

def _count_working_days(start: date, end: date) -> int:
    """Count weekdays (Mon-Fri) in [start, end] inclusive."""
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0-4 = Mon-Fri, 5-6 = Sat-Sun
            count += 1
        current += timedelta(days=1)
    return count


def compute_time_allocation(spark: SparkSession, catalog: str, schema: str, as_of: date) -> DataFrame:
    """Compute time allocation metrics for the 30-day window ending as_of.

    Business rules:
    - Feature work hours = story_worklog_hours if > 0, else story_sp_hours_estimate * 0.5 (confidence discount for estimates)
    - Bug fixing hours = bug_worklog_hours if > 0, else bug_sp_hours_estimate * 0.5
    - Code review hours = total_prs_reviewed_30d * CODE_REVIEW_HRS_PER_PR
    - Meeting hours = total_attendee_hours from gcal_gold_daily_meeting_hours (30d sum)
    - Eligible hours = working_days_in_window * unique_engineers * HOURS_PER_WORKING_DAY
    - Other hours = max(0, eligible_hours - feature_hours - bug_hours - review_hours - meeting_hours)
    - If total allocated > eligible_hours, cap each category proportionally
    - coverage_score = 0-100 based on data availability
    """
    window_start = as_of - timedelta(days=29)
    window_end = as_of

    # Count working days in [window_start, window_end]
    working_days = _count_working_days(window_start, window_end)

    # Count unique engineers from github gold (proxy for team size)
    # If no data, use fallback of 5
    try:
        engineer_count_rows = spark.sql(f"""
            SELECT COUNT(DISTINCT developer) as cnt
            FROM `{catalog}`.`{schema}`.github_gold_project_daily_metrics
            WHERE metric_date BETWEEN '{window_start}' AND '{window_end}'
        """).collect()
        engineer_count = engineer_count_rows[0]["cnt"] if engineer_count_rows and engineer_count_rows[0]["cnt"] else 5
    except Exception:
        engineer_count = 5

    engineer_count = max(1, engineer_count)

    # Eligible working hours
    eligible_hours = working_days * engineer_count * HOURS_PER_WORKING_DAY

    # Jira 30d aggregations
    try:
        jira_rows = spark.sql(f"""
            SELECT
                SUM(CAST(story_worklog_hours AS DOUBLE)) as total_story_worklog_hours,
                SUM(CAST(bug_worklog_hours AS DOUBLE)) as total_bug_worklog_hours,
                SUM(CAST(story_sp_hours_estimate AS DOUBLE)) as total_story_sp_hours_estimate,
                SUM(CAST(bug_sp_hours_estimate AS DOUBLE)) as total_bug_sp_hours_estimate,
                SUM(CAST(issues_with_worklogs AS INT)) as total_issues_with_worklogs,
                SUM(CAST(total_issues_updated AS INT)) as total_issues_updated
            FROM `{catalog}`.`{schema}`.jira_gold_project_daily_metrics
            WHERE metric_date BETWEEN '{window_start}' AND '{window_end}'
        """).collect()

        jira_data = jira_rows[0] if jira_rows else {}
        story_worklog_hours = jira_data.get("total_story_worklog_hours") or 0
        bug_worklog_hours = jira_data.get("total_bug_worklog_hours") or 0
        story_sp_hours_estimate = jira_data.get("total_story_sp_hours_estimate") or 0
        bug_sp_hours_estimate = jira_data.get("total_bug_sp_hours_estimate") or 0
        issues_with_worklogs = jira_data.get("total_issues_with_worklogs") or 0
        total_issues_updated = jira_data.get("total_issues_updated") or 0
    except Exception:
        story_worklog_hours = 0
        bug_worklog_hours = 0
        story_sp_hours_estimate = 0
        bug_sp_hours_estimate = 0
        issues_with_worklogs = 0
        total_issues_updated = 0

    # Coverage for Jira
    coverage_jira = issues_with_worklogs / total_issues_updated if total_issues_updated > 0 else 0

    # Feature and bug hours with confidence adjustment
    if story_worklog_hours > 0:
        feature_hours = story_worklog_hours
        feature_source = "worklogs"
    else:
        feature_hours = story_sp_hours_estimate * 0.5  # 50% confidence discount for estimates
        feature_source = "story_point_estimate"

    if bug_worklog_hours > 0:
        bug_hours = bug_worklog_hours
        bug_source = "worklogs"
    else:
        bug_hours = bug_sp_hours_estimate * 0.5
        bug_source = "story_point_estimate"

    # PR review hours from GitHub gold 30d
    try:
        pr_rows = spark.sql(f"""
            SELECT SUM(CAST(reviewed_prs AS INT)) as total_reviewed_prs
            FROM `{catalog}`.`{schema}`.github_gold_pr_daily_metrics
            WHERE metric_date BETWEEN '{window_start}' AND '{window_end}'
        """).collect()

        pr_data = pr_rows[0] if pr_rows else {}
        reviewed_prs = pr_data.get("total_reviewed_prs") or 0
        code_review_hours = reviewed_prs * CODE_REVIEW_HRS_PER_PR
    except Exception:
        reviewed_prs = 0
        code_review_hours = 0

    # Meeting hours from GCal gold 30d
    gcal_available = True
    try:
        # Check if table exists
        spark.table(f"`{catalog}`.`{schema}`.gcal_gold_daily_meeting_hours")
        gcal_rows = spark.sql(f"""
            SELECT SUM(CAST(total_attendee_hours AS DOUBLE)) as total_meeting_hours
            FROM `{catalog}`.`{schema}`.gcal_gold_daily_meeting_hours
            WHERE metric_date BETWEEN '{window_start}' AND '{window_end}'
        """).collect()

        gcal_data = gcal_rows[0] if gcal_rows else {}
        meeting_hours = gcal_data.get("total_meeting_hours") or 0
    except Exception:
        meeting_hours = 0
        gcal_available = False

    # Total allocated (raw)
    total_allocated_raw = feature_hours + bug_hours + code_review_hours + meeting_hours

    # Scale down if over-allocated
    if total_allocated_raw > eligible_hours and total_allocated_raw > 0:
        scale_factor = eligible_hours / total_allocated_raw
        feature_hours = feature_hours * scale_factor
        bug_hours = bug_hours * scale_factor
        code_review_hours = code_review_hours * scale_factor
        meeting_hours = meeting_hours * scale_factor

    total_allocated = feature_hours + bug_hours + code_review_hours + meeting_hours
    other_hours = max(0, eligible_hours - total_allocated)

    # Coverage score (0-100)
    coverage_base = 20 if issues_with_worklogs > 0 else 0  # Jira data quality
    coverage_base += 30 if gcal_available else 0  # Calendar data
    coverage_base += 10 if reviewed_prs > 0 else 0  # PR data
    coverage_score = min(100, coverage_base + 40)  # Base 40 + adjustments

    # Build single-row result
    result_row = spark.createDataFrame(
        [
            {
                "allocation_date": as_of,
                "feature_work_hours": round(feature_hours, 1),
                "bug_fixing_hours": round(bug_hours, 1),
                "code_review_hours": round(code_review_hours, 1),
                "meeting_hours": round(meeting_hours, 1),
                "other_hours": round(other_hours, 1),
                "total_allocated_hours": round(total_allocated, 1),
                "eligible_working_hours": round(eligible_hours, 1),
                "working_days": working_days,
                "engineer_count": engineer_count,
                "feature_source": feature_source,
                "bug_source": bug_source,
                "gcal_available": gcal_available,
                "coverage_score": coverage_score,
                "updated_at": _now(),
            }
        ]
    )

    return result_row


def ensure_time_allocation_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.dashboard_gold_time_allocation (
            allocation_date          DATE     NOT NULL,
            feature_work_hours       DOUBLE,
            bug_fixing_hours         DOUBLE,
            code_review_hours        DOUBLE,
            meeting_hours            DOUBLE,
            other_hours              DOUBLE,
            total_allocated_hours    DOUBLE,
            eligible_working_hours   DOUBLE,
            working_days             INT,
            engineer_count           INT,
            feature_source           STRING,
            bug_source               STRING,
            gcal_available           BOOLEAN,
            coverage_score           INT,
            updated_at               TIMESTAMP
        )
        USING DELTA
        COMMENT 'Daily engineering time allocation: feature work, bugs, reviews, meetings, other'
    """)


def merge_time_allocation(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_dashboard_time_allocation_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.dashboard_gold_time_allocation AS tgt
        USING _dashboard_time_allocation_staging AS src
        ON tgt.allocation_date = src.allocation_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def process_dashboard(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> None:
    as_of = _resolve_date(processing_date)
    print(f"Dashboard Gold: computing mart as of {as_of} for {catalog}.{schema}")

    # Project summary (must run before KPIs so health score is available)
    ensure_project_summary_table(spark, catalog, schema)
    summary_df = compute_project_summary(spark, catalog, schema, as_of)
    merge_project_summary(spark, summary_df, catalog, schema)
    print(f"Dashboard Gold: project_summary — {summary_df.count()} projects")

    # KPIs (reads project summary for org health score)
    ensure_kpis_table(spark, catalog, schema)
    kpis_df = compute_kpis(spark, catalog, schema, as_of)
    merge_kpis(spark, kpis_df, catalog, schema)
    print("Dashboard Gold: kpis — updated")

    # Time allocation
    ensure_time_allocation_table(spark, catalog, schema)
    time_alloc_df = compute_time_allocation(spark, catalog, schema, as_of)
    merge_time_allocation(spark, time_alloc_df, catalog, schema)
    print("Dashboard Gold: time_allocation — updated")

    # Recent activity
    ensure_activity_table(spark, catalog, schema)
    activity_df = compute_recent_activity(spark, catalog, schema)
    refresh_activity(spark, activity_df, catalog, schema)
    print(f"Dashboard Gold: recent_activity — {activity_df.count()} events")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dashboard Gold Mart")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_dashboard(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
