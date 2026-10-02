"""Silver layer: GitHub deployment validation and normalization.

Reads from github_bronze_project_deployments, validates fields, normalizes
environment names, and marks successful production deployments.

Entry point: project_github_deployment_silver
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_VALID_ENVS = {"production", "staging", "development", "preview", "qa"}
_VALID_STATUSES = {"success", "failure", "error", "pending", "in_progress"}


def transform_silver(df: DataFrame) -> DataFrame:
    df = df.filter(
        F.col("deployment_id").isNotNull()
        & F.col("organization").isNotNull()
        & F.col("repository").isNotNull()
        & F.col("created_at").isNotNull()
    )

    df = (
        df.withColumn("creator",       F.trim("creator"))
          .withColumn("creator_email", F.lower(F.trim("creator_email")))
          .withColumn("environment",   F.lower(F.trim("environment")))
          .withColumn("status",        F.lower(F.trim("status")))
    )

    df = df.withColumn(
        "duration_seconds",
        F.greatest(F.col("duration_seconds"), F.lit(0)),
    )

    df = (
        df.withColumn("deploy_date",         F.to_date("created_at"))
          .withColumn("is_production",        F.col("environment") == "production")
          .withColumn("is_successful",        F.col("status") == "success")
          .withColumn("is_successful_prod",   F.col("is_production") & F.col("is_successful"))
    )

    # Deduplicate: keep most recent processing_date per deployment_id
    w = Window.partitionBy("deployment_id").orderBy(F.col("processing_date").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    return df.withColumn("processed_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_silver_project_deployments"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            deployment_id        STRING NOT NULL,
            repository           STRING NOT NULL,
            organization         STRING NOT NULL,
            project_id           STRING,
            project_name         STRING,
            environment          STRING,
            ref                  STRING,
            sha                  STRING,
            created_at           TIMESTAMP,
            updated_at           TIMESTAMP,
            status               STRING,
            creator              STRING,
            creator_email        STRING,
            description          STRING,
            duration_seconds     INT,
            deploy_date          DATE,
            is_production        BOOLEAN,
            is_successful        BOOLEAN,
            is_successful_prod   BOOLEAN,
            processing_date      DATE,
            processed_at         TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (deploy_date)
        COMMENT 'Validated GitHub deployments with production/success flags'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_deployment_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_deployment_silver_staging AS src
        ON tgt.deployment_id = src.deployment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import timedelta
        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_silver(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    bronze_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.github_bronze_project_deployments
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"Deployment Silver: no records for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"Deployment Silver: merged {count} deployments for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Deployment Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_silver(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
