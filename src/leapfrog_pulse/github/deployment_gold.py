"""Gold layer: daily deployment metrics per project.

Reads from github_silver_project_deployments and aggregates into daily
metrics powering the Deployment Frequency KPI on the dashboard.

Entry point: project_github_deployment_gold
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(
            F.col("deploy_date").alias("metric_date"),
            "project_id",
            "project_name",
            "organization",
            "repository",
        )
        .agg(
            F.count("deployment_id").alias("total_deployments"),
            F.sum(F.when(F.col("is_successful"), 1).otherwise(0)).alias("successful_deployments"),
            F.sum(F.when(F.col("is_production"), 1).otherwise(0)).alias("production_deployments"),
            F.sum(F.when(F.col("is_successful_prod"), 1).otherwise(0)).alias("successful_prod_deployments"),
            F.sum(F.when(~F.col("is_successful"), 1).otherwise(0)).alias("failed_deployments"),
            F.round(F.avg(F.col("duration_seconds")), 0).cast("int").alias("avg_duration_seconds"),
            F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("updated_at"),
        )
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_gold_deployment_daily_metrics"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date                 DATE    NOT NULL,
            project_id                  STRING,
            project_name                STRING,
            organization                STRING  NOT NULL,
            repository                  STRING  NOT NULL,
            total_deployments           LONG,
            successful_deployments      LONG,
            production_deployments      LONG,
            successful_prod_deployments LONG,
            failed_deployments          LONG,
            avg_duration_seconds        INT,
            updated_at                  TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
        COMMENT 'Daily deployment metrics per project: frequency and success rate'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_deployment_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_deployment_gold_staging AS src
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
        SELECT * FROM `{catalog}`.`{schema}`.github_silver_project_deployments
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"Deployment Gold: no records for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"Deployment Gold: merged {count} rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Deployment Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_gold(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
