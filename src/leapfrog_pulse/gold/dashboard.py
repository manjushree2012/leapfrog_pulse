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
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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

    # JIRA: latest daily snapshot per project (most recent metric_date)
    w = Window.partitionBy("project_id").orderBy(F.col("metric_date").desc())
    jr_latest = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "project_id",
            "open_issues",
            "in_progress_issues",
            "done_issues",
            "bugs_count",
            "stories_count",
            "tasks_count",
            "avg_story_points",
            "sprint_name",
        )
    )

    # JIRA: 30-day done count (velocity proxy)
    jr_30d = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .groupBy("project_id")
        .agg(F.sum("done_issues").alias("done_issues_last_30d"))
    )

    # Join everything — Vyaguta is the master project list
    result = (
        vy_projects
        .join(gh, "project_id", "left")
        .join(jr_latest, "project_id", "left")
        .join(jr_30d, "project_id", "left")
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
            health_score        INT,
            as_of_date          DATE,
            updated_at          TIMESTAMP
        )
        USING DELTA
        COMMENT '30-day rolling project health summary for the DevPulse dashboard'
    """)


def merge_project_summary(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
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

    # JIRA org-wide latest snapshot
    w = Window.partitionBy("project_key").orderBy(F.col("metric_date").desc())
    jr = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .agg(
            F.sum("open_issues").alias("total_open_issues"),
            F.sum("in_progress_issues").alias("total_in_progress"),
            F.sum("done_issues").alias("total_done_issues"),
            F.sum("bugs_count").alias("total_bugs"),
            F.sum("stories_count").alias("total_stories"),
            F.sum("tasks_count").alias("total_tasks"),
        )
    )

    # JIRA 30-day velocity
    jr_30d = (
        spark.table(f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics")
        .filter(F.col("metric_date").between(window_start, window_end))
        .agg(F.sum("done_issues").alias("done_issues_30d"))
    )

    # Project summary health average
    ph = spark.table(f"`{catalog}`.`{schema}`.dashboard_gold_project_summary").agg(
        F.round(F.avg("health_score"), 0).cast("int").alias("org_health_score"),
        F.count("project_id").alias("tracked_projects"),
    )

    # Cross-join single-row aggregations to produce one KPI row
    kpis = gh.crossJoin(jr).crossJoin(jr_30d).crossJoin(ph)

    return kpis.withColumn("kpi_date", F.lit(as_of.isoformat()).cast("date")).withColumn(
        "updated_at", F.lit(_now()).cast("timestamp")
    )


def ensure_kpis_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.dashboard_gold_kpis (
            kpi_date            DATE    NOT NULL,
            total_commits_30d   LONG,
            active_developers   LONG,
            active_projects     LONG,
            active_repos        LONG,
            total_open_issues   LONG,
            total_in_progress   LONG,
            total_done_issues   LONG,
            done_issues_30d     LONG,
            total_bugs          LONG,
            total_stories       LONG,
            total_tasks         LONG,
            org_health_score    INT,
            tracked_projects    LONG,
            updated_at          TIMESTAMP
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
