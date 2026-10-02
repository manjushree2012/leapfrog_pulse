"""Gold layer: daily PR metrics per project — review time and merge rate.

Reads from github_silver_project_prs and aggregates into daily metrics
powering the PR Review Time KPI on the dashboard.

Entry point: project_github_pr_gold
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(
            F.col("pr_date").alias("metric_date"),
            "project_id",
            "project_name",
            "organization",
            "repository",
        )
        .agg(
            F.count("pr_id").alias("total_prs"),
            F.sum(F.when(F.col("is_merged"), 1).otherwise(0)).alias("merged_prs"),
            F.sum(F.when(F.col("first_review_submitted_at").isNotNull(), 1).otherwise(0)).alias("reviewed_prs"),
            # avg review time in hours for reviewed PRs (non-null review_time_seconds)
            F.round(
                F.avg(F.when(F.col("review_time_seconds").isNotNull(),
                             F.col("review_time_seconds") / 3600.0)),
                2,
            ).alias("avg_review_time_hrs"),
            F.round(
                F.avg(F.when(
                    F.col("merged_at").isNotNull() & F.col("created_at").isNotNull(),
                    (F.unix_timestamp("merged_at") - F.unix_timestamp("created_at")) / 3600.0,
                )),
                2,
            ).alias("avg_merge_time_hrs"),
            F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("updated_at"),
        )
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_gold_pr_daily_metrics"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date         DATE    NOT NULL,
            project_id          STRING,
            project_name        STRING,
            organization        STRING  NOT NULL,
            repository          STRING  NOT NULL,
            total_prs           LONG,
            merged_prs          LONG,
            reviewed_prs        LONG,
            avg_review_time_hrs DOUBLE,
            avg_merge_time_hrs  DOUBLE,
            updated_at          TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
        COMMENT 'Daily GitHub PR metrics: review time and merge rate per project'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_pr_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_pr_gold_staging AS src
        ON  tgt.metric_date  = src.metric_date
        AND tgt.organization = src.organization
        AND tgt.repository   = src.repository
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import timedelta
        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_gold(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    silver_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.github_silver_project_prs
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"PR Gold: no records for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"PR Gold: merged {count} rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub PR Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_gold(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
