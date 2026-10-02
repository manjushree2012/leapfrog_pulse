"""Jira Gold layer — daily project-level issue analytics.

Reads from jira_silver_issues and aggregates into one row per
(processing_date, project) capturing issue counts broken down by status,
type, and resolution. This feeds the dashboard's team/project panels and
the "Where Engineering Time Goes" donut chart.

Entry point: jira_gold
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    """Aggregate silver issues into daily per-project metrics."""
    return (
        df.groupBy(
            F.col("processing_date").alias("metric_date"),
            "project_id",
            "project_name",
            "project_key",
            "sprint_name",
        )
        .agg(
            F.count("issue_key").alias("total_issues_updated"),
            F.sum(F.when(F.col("status") == "To Do", 1).otherwise(0)).alias("open_issues"),
            F.sum(F.when(F.col("status").isin("In Progress", "In Review"), 1).otherwise(0)).alias("in_progress_issues"),
            F.sum(F.when(F.col("status") == "Done", 1).otherwise(0)).alias("done_issues"),
            F.sum(F.when(F.col("is_bug"), 1).otherwise(0)).alias("bugs_count"),
            F.sum(F.when(F.col("issue_type") == "Story", 1).otherwise(0)).alias("stories_count"),
            F.sum(F.when(F.col("issue_type") == "Task", 1).otherwise(0)).alias("tasks_count"),
            F.sum(F.when(F.col("issue_type") == "Sub-task", 1).otherwise(0)).alias("subtasks_count"),
            F.round(F.avg(F.col("story_points")), 1).alias("avg_story_points"),
            F.sum(F.when(F.col("is_resolved"), F.coalesce("story_points", F.lit(0))).otherwise(0)).alias("resolved_story_points"),
            F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("updated_at"),
        )
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.jira_gold_project_daily_metrics"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date             DATE    NOT NULL,
            project_id              STRING,
            project_name            STRING,
            project_key             STRING  NOT NULL,
            sprint_name             STRING,
            total_issues_updated    INT,
            open_issues             INT,
            in_progress_issues      INT,
            done_issues             INT,
            bugs_count              INT,
            stories_count           INT,
            tasks_count             INT,
            subtasks_count          INT,
            avg_story_points        DOUBLE,
            resolved_story_points   INT,
            updated_at              TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
        COMMENT 'Daily JIRA project metrics: issue counts by status and type, story point velocity'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_jira_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _jira_gold_staging AS src
        ON  tgt.metric_date = src.metric_date
        AND tgt.project_key = src.project_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import date, timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_gold(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    silver_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.jira_silver_issues
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"JIRA Gold: no records to process for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"JIRA Gold: merged {count} metric rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="JIRA Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_gold(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
