"""Jira Silver layer — cleaned and validated JIRA issues.

Reads from jira_bronze_issues, enforces data quality, normalizes values, and
writes to jira_silver_issues.

Validation rules:
  - Drop rows missing issue_key or project_key
  - Normalize status to strict whitelist: "To Do", "In Progress", "In Review", "Done"
  - Normalize issue_type to strict whitelist: "Story", "Bug", "Task", "Sub-task"
  - Normalize priority to strict whitelist: "Critical", "High", "Medium", "Low"
  - Null story_points outside [1, 100]
  - Deduplicate by (issue_key, processing_date): keep latest updated_at
  - Extract created_date, updated_date as DATE columns
  - Derive is_resolved and is_bug flags

Entry point: jira_silver
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_VALID_STATUSES = ["To Do", "In Progress", "In Review", "Done"]
_VALID_ISSUE_TYPES = ["Story", "Bug", "Task", "Sub-task"]
_VALID_PRIORITIES = ["Critical", "High", "Medium", "Low"]


def transform_silver(df: DataFrame) -> DataFrame:
    """Pure transformation from bronze to silver — no I/O."""
    df = df.filter(
        F.col("issue_key").isNotNull()
        & (F.trim(F.col("issue_key")) != "")
        & F.col("project_key").isNotNull()
    )

    df = (
        df.withColumn("issue_key", F.trim("issue_key"))
        .withColumn("project_key", F.upper(F.trim("project_key")))
        .withColumn("project_name", F.trim("project_name"))
        .withColumn("summary", F.trim("summary"))
        .withColumn("assignee", F.trim("assignee"))
        .withColumn("reporter", F.trim("reporter"))
        .withColumn("sprint_name", F.trim("sprint_name"))
    )

    # Enforce status whitelist — null out unrecognized values rather than dropping
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(_VALID_STATUSES), F.col("status")).otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "issue_type",
        F.when(F.col("issue_type").isin(_VALID_ISSUE_TYPES), F.col("issue_type")).otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "priority",
        F.when(F.col("priority").isin(_VALID_PRIORITIES), F.col("priority")).otherwise(F.lit(None)),
    )

    # story_points must be a small positive integer
    df = df.withColumn(
        "story_points",
        F.when((F.col("story_points") >= 1) & (F.col("story_points") <= 100), F.col("story_points")).otherwise(F.lit(None)),
    )

    # Extract date parts
    df = (
        df.withColumn("created_date", F.to_date("created_at"))
        .withColumn("updated_date", F.to_date("updated_at"))
    )

    # Derived semantic flags
    df = (
        df.withColumn("is_resolved", F.col("status") == "Done")
        .withColumn("is_bug", F.col("issue_type") == "Bug")
    )

    # Deduplicate: keep the record with the latest updated_at per (issue_key, processing_date)
    window = Window.partitionBy("issue_key", "processing_date").orderBy(F.col("updated_at").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    processed_at = datetime.now(timezone.utc)
    df = df.withColumn("processed_at", F.lit(processed_at).cast("timestamp"))

    return df.select(
        "issue_key",
        "project_key",
        "project_id",
        "project_name",
        "summary",
        "status",
        "issue_type",
        "priority",
        "assignee",
        "reporter",
        "created_at",
        "created_date",
        "updated_at",
        "updated_date",
        "story_points",
        "sprint_name",
        "worklog_hours",
        "has_worklogs",
        "is_resolved",
        "is_bug",
        "processing_date",
        "processed_at",
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.jira_silver_issues"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            issue_key       STRING  NOT NULL,
            project_key     STRING  NOT NULL,
            project_id      STRING,
            project_name    STRING,
            summary         STRING,
            status          STRING,
            issue_type      STRING,
            priority        STRING,
            assignee        STRING,
            reporter        STRING,
            created_at      TIMESTAMP,
            created_date    DATE,
            updated_at      TIMESTAMP,
            updated_date    DATE,
            story_points    INT,
            sprint_name     STRING,
            worklog_hours   DOUBLE,
            has_worklogs    BOOLEAN,
            is_resolved     BOOLEAN,
            is_bug          BOOLEAN,
            processing_date DATE,
            processed_at    TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'JIRA issues — validated and normalized, one snapshot per (issue, processing date)'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_jira_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _jira_silver_staging AS src
        ON tgt.issue_key = src.issue_key AND tgt.processing_date = src.processing_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import date, timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_silver(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    bronze_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.jira_bronze_issues
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"JIRA Silver: no records to process for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"JIRA Silver: merged {count} issues for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="JIRA Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_silver(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
