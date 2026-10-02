"""Jira Bronze layer — raw ingestion of JIRA issues updated in the past 24 hours.

Reads the project list from the Vyaguta bronze table, then fetches issues updated
on processing_date for each unique Jira project key. Writes to jira_bronze_issues.

Entry point: jira_bronze
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

from leapfrog_pulse.jira.mock_source import get_issues_for_project_and_date

_RAW_SCHEMA = StructType(
    [
        StructField("issue_key", StringType(), False),
        StructField("project_key", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("status", StringType(), True),
        StructField("issue_type", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("assignee", StringType(), True),
        StructField("reporter", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("story_points", IntegerType(), True),
        StructField("sprint_name", StringType(), True),
    ]
)


def _load_projects(spark: SparkSession, catalog: str, schema: str) -> list[dict]:
    """Read distinct (project_id, project_name, jira_project_key) from Vyaguta bronze."""
    rows = spark.sql(f"""
        SELECT DISTINCT project_id, project_name, jira_project_key
        FROM `{catalog}`.`{schema}`.`vyaguta_bronze_projects`
        WHERE jira_project_key IS NOT NULL
    """).collect()
    return [r.asDict() for r in rows]


def build_bronze_df(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> DataFrame:
    projects = _load_projects(spark, catalog, schema)

    all_issues: list[dict] = []
    for row in projects:
        issues = get_issues_for_project_and_date(row["jira_project_key"], processing_date)
        for issue in issues:
            issue["project_id"] = row["project_id"]
            issue["project_name"] = row["project_name"]
            all_issues.append(issue)

    if not all_issues:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(all_issues, schema=_RAW_SCHEMA)
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("processing_date", F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.jira_bronze_issues"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            issue_key           STRING NOT NULL,
            project_key         STRING,
            project_id          STRING,
            project_name        STRING,
            summary             STRING,
            status              STRING,
            issue_type          STRING,
            priority            STRING,
            assignee            STRING,
            reporter            STRING,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP,
            story_points        INT,
            sprint_name         STRING,
            ingestion_timestamp TIMESTAMP,
            processing_date     DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Raw JIRA issues updated in the past 24 hours, enriched with Vyaguta project context'
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    """Idempotent MERGE: upsert on (issue_key, processing_date) so re-runs never duplicate."""
    df.createOrReplaceTempView("_jira_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _jira_bronze_staging AS src
        ON tgt.issue_key = src.issue_key AND tgt.processing_date = src.processing_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def ingest_bronze(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    df = build_bronze_df(spark, catalog, schema, processing_date)
    count = df.count()
    if count == 0:
        print(f"JIRA Bronze: no issues found for {processing_date}")
        return 0
    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"JIRA Bronze: merged {count} issues for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="JIRA Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    ingest_bronze(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
