"""Vyaguta Bronze layer — raw ingestion of project and team-member metadata.

Vyaguta data is relatively static, so this layer is run once (or on-demand)
rather than on a daily schedule. It ingests the project catalogue and member
assignments from the mock source (or real Vyaguta API) into separate bronze
tables.

Entry point:
    uv run python -m leapfrog_pulse.vyaguta.bronze --catalog <catalog> --schema <schema>
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from leapfrog_pulse.vyaguta.mock_source import get_all_projects, get_project_members

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

_MEMBERS_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=False),
        StructField("employee_name", StringType(), nullable=True),
        StructField("employee_email", StringType(), nullable=False),
        StructField("role", StringType(), nullable=True),
        StructField("company", StringType(), nullable=True),
    ]
)


def build_bronze_df(spark: SparkSession) -> DataFrame:
    """Load all Vyaguta project-repo rows into a DataFrame with ingestion metadata."""
    from pyspark.sql import functions as F

    rows = get_all_projects()
    df = spark.createDataFrame(rows, schema=_RAW_SCHEMA)
    now = datetime.now(tz=timezone.utc)
    return df.withColumn("ingestion_timestamp", F.lit(now).cast(TimestampType()))


def build_members_bronze_df(spark: SparkSession) -> DataFrame:
    """Load Vyaguta project-member rows, including employee email addresses."""
    from pyspark.sql import functions as F

    rows = get_project_members()
    df = spark.createDataFrame(rows, schema=_MEMBERS_SCHEMA)
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
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`vyaguta_bronze_project_members` (
            project_id          STRING NOT NULL,
            employee_name      STRING,
            employee_email     STRING NOT NULL,
            role               STRING,
            company            STRING,
            ingestion_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Vyaguta project-member assignments, including employee email addresses'
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


def merge_members_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_vyaguta_bronze_members_staging")
    spark.sql(f"""
        MERGE INTO `{catalog}`.`{schema}`.`vyaguta_bronze_project_members` AS target
        USING _vyaguta_bronze_members_staging AS source
        ON target.project_id = source.project_id AND target.employee_email = source.employee_email
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def ingest_bronze(spark: SparkSession, catalog: str, schema: str) -> int:
    ensure_bronze_table(spark, catalog, schema)
    df = build_bronze_df(spark)
    members_df = build_members_bronze_df(spark)
    merge_bronze(spark, df, catalog, schema)
    merge_members_bronze(spark, members_df, catalog, schema)
    print(f"Vyaguta Bronze: upserted {members_df.count()} project-member rows.")
    return df.count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Vyaguta Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_known_args()[0]

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    count = ingest_bronze(spark, args.catalog, args.schema)
    print(f"Vyaguta Bronze: upserted {count} project-repo rows.")


if __name__ == "__main__":
    main()
