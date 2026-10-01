"""Vyaguta Silver layer — cleaned and normalized project/repository metadata.

Reads from the Bronze table, validates and normalises records, and writes to
the Silver table. Run this immediately after bronze ingestion.

Entry point:
    uv run python -m leapfrog_pulse.vyaguta.silver --catalog <catalog> --schema <schema>
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def transform_silver(df: DataFrame) -> DataFrame:
    """Pure, I/O-free transformation from Bronze to Silver."""
    return (
        df
        # Drop rows missing mandatory keys
        .filter(F.col("project_id").isNotNull() & F.col("source_hash").isNotNull())
        .filter(F.col("github_repo_url").isNotNull() & (F.trim(F.col("github_repo_url")) != ""))
        # Normalise strings
        .withColumn("project_name", F.trim(F.col("project_name")))
        .withColumn("team", F.trim(F.col("team")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("github_repo_url", F.trim(F.col("github_repo_url")))
        .withColumn("jira_project_key", F.upper(F.trim(F.col("jira_project_key"))))
        .withColumn("company", F.lower(F.trim(F.col("company"))))
        # Dedup on source_hash — keep the latest ingestion if duplicates somehow exist
        .dropDuplicates(["source_hash"])
        # Extract owner and repo name from github_repo_url for convenience
        .withColumn(
            "_url_parts",
            F.split(F.regexp_replace(F.col("github_repo_url"), r"https?://github\.com/", ""), "/"),
        )
        .withColumn("github_owner", F.col("_url_parts").getItem(0))
        .withColumn("github_repo_name", F.col("_url_parts").getItem(1))
        .drop("_url_parts")
        .withColumn("processed_at", F.lit(datetime.now(tz=timezone.utc)).cast(TimestampType()))
        # Fixed column projection
        .select(
            "project_id",
            "project_name",
            "team",
            "status",
            "github_repo_url",
            "github_owner",
            "github_repo_name",
            "jira_project_key",
            "company",
            "source_hash",
            "ingestion_timestamp",
            "processed_at",
        )
    )


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`vyaguta_silver_projects` (
            project_id        STRING  NOT NULL,
            project_name      STRING,
            team              STRING,
            status            STRING,
            github_repo_url   STRING,
            github_owner      STRING,
            github_repo_name  STRING,
            jira_project_key  STRING,
            company           STRING,
            source_hash       STRING  NOT NULL,
            ingestion_timestamp TIMESTAMP,
            processed_at      TIMESTAMP
        )
        USING DELTA
        COMMENT 'Vyaguta project-to-repository mapping — cleaned and normalised'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_vyaguta_silver_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.`vyaguta_silver_projects` AS target
        USING _vyaguta_silver_staging AS source
        ON target.source_hash = source.source_hash
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def process_silver(spark: SparkSession, catalog: str, schema: str) -> int:
    ensure_silver_table(spark, catalog, schema)
    bronze_df = spark.table(f"`{catalog}`.`{schema}`.`vyaguta_bronze_projects`")
    silver_df = transform_silver(bronze_df)
    merge_silver(spark, silver_df, catalog, schema)
    return silver_df.count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Vyaguta Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    count = process_silver(spark, args.catalog, args.schema)
    print(f"Vyaguta Silver: upserted {count} project-repo rows.")


if __name__ == "__main__":
    main()
