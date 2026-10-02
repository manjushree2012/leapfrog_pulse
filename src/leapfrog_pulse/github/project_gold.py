"""GitHub project-driven Gold layer.

Reads from github_silver_project_commits and aggregates into daily metrics
per (project, repository, developer). This gold table powers dashboard KPIs
for commit activity, code churn, and per-developer contribution per project.

Entry point: project_github_gold
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    """Aggregate silver commits into daily per-(project, repo, developer) metrics."""
    return (
        df.groupBy(
            F.col("commit_date").alias("metric_date"),
            "project_id",
            "project_name",
            "organization",
            "repository",
            F.col("author").alias("developer"),
            F.col("author_email").alias("developer_email"),
        )
        .agg(
            F.count("commit_id").alias("commit_count"),
            F.sum("additions").alias("total_additions"),
            F.sum("deletions").alias("total_deletions"),
            F.sum("changed_files").alias("total_changed_files"),
            F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("updated_at"),
        )
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_gold_project_daily_metrics"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date         DATE    NOT NULL,
            project_id          STRING  NOT NULL,
            project_name        STRING,
            organization        STRING  NOT NULL,
            repository          STRING  NOT NULL,
            developer           STRING  NOT NULL,
            developer_email     STRING,
            commit_count        LONG,
            total_additions     LONG,
            total_deletions     LONG,
            total_changed_files LONG,
            updated_at          TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
        COMMENT 'Daily GitHub commit metrics per developer, repository, and project'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_project_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_project_gold_staging AS src
        ON  tgt.metric_date   = src.metric_date
        AND tgt.project_id    = src.project_id
        AND tgt.organization  = src.organization
        AND tgt.repository    = src.repository
        AND tgt.developer     = src.developer
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
        SELECT * FROM `{catalog}`.`{schema}`.github_silver_project_commits
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"Project GitHub Gold: no records to process for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"Project GitHub Gold: merged {count} metric rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Project GitHub Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_gold(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
