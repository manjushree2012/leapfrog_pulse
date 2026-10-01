"""Vyaguta Gold layer — analytics-facing project catalogue.

Reads from the Silver table and produces a clean, deduplicated project
catalogue that other pipelines (e.g. GitHub, Jira) can join on.
The Gold table is the authoritative source for:
  - github_repo_url  → used to seed per-repo GitHub commit ingestion
  - jira_project_key → used to seed per-project Jira issue ingestion

Entry point:
    uv run python -m leapfrog_pulse.vyaguta.gold --catalog <catalog> --schema <schema>
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def transform_gold(df: DataFrame) -> DataFrame:
    """Project the Silver data into the analytics-ready catalogue shape."""
    return (
        df
        .filter(F.col("status") == "active")
        .select(
            "project_id",
            "project_name",
            "team",
            "github_repo_url",
            "github_owner",
            "github_repo_name",
            "jira_project_key",
            "company",
            "source_hash",
        )
        .dropDuplicates(["source_hash"])
        .withColumn("updated_at", F.lit(datetime.now(tz=timezone.utc)).cast(TimestampType()))
    )


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`vyaguta_gold_project_catalogue` (
            project_id        STRING  NOT NULL,
            project_name      STRING,
            team              STRING,
            github_repo_url   STRING,
            github_owner      STRING,
            github_repo_name  STRING,
            jira_project_key  STRING,
            company           STRING,
            source_hash       STRING  NOT NULL,
            updated_at        TIMESTAMP
        )
        USING DELTA
        COMMENT 'Vyaguta active project catalogue — seed table for GitHub and Jira pipelines'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_vyaguta_gold_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.`vyaguta_gold_project_catalogue` AS target
        USING _vyaguta_gold_staging AS source
        ON target.source_hash = source.source_hash
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def process_gold(spark: SparkSession, catalog: str, schema: str) -> int:
    ensure_gold_table(spark, catalog, schema)
    silver_df = spark.table(f"`{catalog}`.`{schema}`.`vyaguta_silver_projects`")
    gold_df = transform_gold(silver_df)
    merge_gold(spark, gold_df, catalog, schema)
    return gold_df.count()


def get_active_repos(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    """Convenience helper: return all github_repo_url values from the Gold catalogue.

    GitHub and Jira ingestion pipelines call this to discover which repos/projects
    to monitor without hard-coding the list.
    """
    rows = (
        spark.table(f"`{catalog}`.`{schema}`.`vyaguta_gold_project_catalogue`")
        .select("github_repo_url")
        .distinct()
        .collect()
    )
    return [row["github_repo_url"] for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Vyaguta Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    count = process_gold(spark, args.catalog, args.schema)
    print(f"Vyaguta Gold: upserted {count} active project-repo rows.")


if __name__ == "__main__":
    main()
