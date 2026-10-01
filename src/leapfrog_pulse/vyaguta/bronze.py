"""Vyaguta Bronze layer — raw ingestion of project/repository metadata.

Vyaguta data is relatively static, so this layer is run once (or on-demand)
rather than on a daily schedule. It ingests the full project catalogue from
the mock source (or real Vyaguta API) and upserts into the bronze table.

Entry point:
    uv run python -m leapfrog_pulse.vyaguta.bronze --catalog <catalog> --schema <schema>
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from leapfrog_pulse.vyaguta.mock_source import get_all_projects

_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=False),
        StructField("project_name", StringType(), nullable=True),
        StructField("team", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("github_repo_url", StringType(), nullable=True),
        StructField("jira_project_key", StringType(), nullable=True),
        StructField("company", StringType(), nullable=True),
        StructField("source_hash", StringType(), nullable=False),
    ]
)


def build_bronze_df(spark: SparkSession) -> DataFrame:
    """Load all Vyaguta project-repo rows into a DataFrame with ingestion metadata."""
    from pyspark.sql import functions as F

    rows = get_all_projects()
    df = spark.createDataFrame(rows, schema=_RAW_SCHEMA)
    now = datetime.now(tz=timezone.utc)
    return df.withColumn("ingestion_timestamp", F.lit(now).cast(TimestampType()))


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`vyaguta_bronze_projects` (
            project_id        STRING  NOT NULL,
            project_name      STRING,
            team              STRING,
            status            STRING,
            github_repo_url   STRING,
            jira_project_key  STRING,
            company           STRING,
            source_hash       STRING  NOT NULL,
            ingestion_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Vyaguta project-to-repository mapping — raw ingestion layer'
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_vyaguta_bronze_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.`vyaguta_bronze_projects` AS target
        USING _vyaguta_bronze_staging AS source
        ON target.source_hash = source.source_hash
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def ingest_bronze(spark: SparkSession, catalog: str, schema: str) -> int:
    ensure_bronze_table(spark, catalog, schema)
    df = build_bronze_df(spark)
    merge_bronze(spark, df, catalog, schema)
    return df.count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Vyaguta Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    count = ingest_bronze(spark, args.catalog, args.schema)
    print(f"Vyaguta Bronze: upserted {count} project-repo rows.")


if __name__ == "__main__":
    main()
