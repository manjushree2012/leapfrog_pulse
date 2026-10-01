"""Gold layer: daily engineering productivity metrics from Silver commits.

Responsibility: aggregate Silver commit events into analytics-ready daily metrics
per (metric_date, organization, repository, developer). This table is the primary
source for dashboard queries — it is designed for easy slicing by date, repo, and dev.

Design decision: one table (github_gold_daily_metrics) covers both repository-level
and developer-level dimensions. Callers can GROUP BY any subset of dimensions.
Adding a separate repository-level rollup table is a future optimization if needed.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    """Aggregate a Silver commits DataFrame into daily developer metrics.

    Pure transformation: no I/O, fully testable without a Delta table.
    Input df is expected to be filtered to a single processing_date already.
    """
    updated_at = datetime.now(timezone.utc)

    return (
        df.groupBy("commit_date", "organization", "repository", "author")
        .agg(
            F.count("commit_id").alias("commit_count"),
            F.sum("additions").alias("additions"),
            F.sum("deletions").alias("deletions"),
            F.sum("changed_files").alias("changed_files"),
        )
        .withColumnRenamed("commit_date", "metric_date")
        .withColumnRenamed("author", "developer")
        .withColumn("additions", F.col("additions").cast("long"))
        .withColumn("deletions", F.col("deletions").cast("long"))
        .withColumn("changed_files", F.col("changed_files").cast("long"))
        .withColumn("updated_at", F.lit(updated_at).cast("timestamp"))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_gold_daily_metrics"


def _silver_table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_silver_commits"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date    DATE NOT NULL,
            organization   STRING NOT NULL,
            repository     STRING NOT NULL,
            developer      STRING NOT NULL,
            commit_count   LONG,
            additions      LONG,
            deletions      LONG,
            changed_files  LONG,
            updated_at     TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    """Idempotent MERGE on composite key (metric_date, organization, repository, developer).

    Re-running the same processing_date overwrites existing metrics rather than
    appending, so counts are never doubled.
    """
    df.createOrReplaceTempView("_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _gold_staging AS src
        ON  tgt.metric_date  = src.metric_date
        AND tgt.organization = src.organization
        AND tgt.repository   = src.repository
        AND tgt.developer    = src.developer
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import date, timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_gold(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    """Full Gold aggregation step for processing_date. Returns output record count."""
    processing_date = _resolve_date(processing_date)
    silver_df = spark.sql(f"""
        SELECT * FROM {_silver_table(catalog, schema)}
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"Gold: no records to aggregate for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"Gold: merged {count} daily metric rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Gold aggregation")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")

    if args.processing_date == "yesterday":
        from datetime import date, timedelta

        processing_date = (date.today() - timedelta(days=1)).isoformat()
    else:
        processing_date = args.processing_date

    process_gold(spark, args.catalog, args.schema, processing_date)


if __name__ == "__main__":
    main()
