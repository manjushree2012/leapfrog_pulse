"""Bronze layer: project-driven GitHub deployment ingestion.

Reads the active project-to-repository mapping from the Vyaguta bronze table,
then fetches deployments created on processing_date for each repo.
Writes to github_bronze_project_deployments.

Entry point: project_github_deployment_bronze
"""

import argparse
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from leapfrog_pulse.github.mock_source import get_deployments_for_repo_and_date

_RAW_SCHEMA = StructType([
    StructField("deployment_id",    StringType(),  False),
    StructField("repository",       StringType(),  False),
    StructField("organization",     StringType(),  False),
    StructField("project_id",      StringType(),  True),
    StructField("project_name",    StringType(),  True),
    StructField("environment",      StringType(),  True),
    StructField("ref",              StringType(),  True),
    StructField("sha",              StringType(),  True),
    StructField("created_at",      StringType(),  True),
    StructField("updated_at",      StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("creator",          StringType(),  True),
    StructField("creator_email",    StringType(),  True),
    StructField("description",      StringType(),  True),
    StructField("duration_seconds", IntegerType(), True),
])


def _parse_repo_url(url: str) -> tuple[str, str]:
    parts = urlparse(url).path.strip("/").split("/")
    return parts[0], parts[1]


def _load_project_repos(spark: SparkSession, catalog: str, schema: str) -> list[dict]:
    rows = spark.sql(f"""
        SELECT DISTINCT project_id, project_name, github_repo_url
        FROM `{catalog}`.`{schema}`.`vyaguta_bronze_projects`
        WHERE github_repo_url IS NOT NULL
    """).collect()
    return [r.asDict() for r in rows]


def build_bronze_df(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> DataFrame:
    repos = _load_project_repos(spark, catalog, schema)

    all_deployments: list[dict] = []
    for row in repos:
        org, repo_name = _parse_repo_url(row["github_repo_url"])
        deploys = get_deployments_for_repo_and_date(repo_name, org, processing_date)
        for d in deploys:
            d["project_id"] = row["project_id"]
            d["project_name"] = row["project_name"]
            all_deployments.append(d)

    if not all_deployments:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(all_deployments, schema=_RAW_SCHEMA)
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("processing_date",     F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_bronze_project_deployments"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            deployment_id    STRING NOT NULL,
            repository       STRING NOT NULL,
            organization     STRING NOT NULL,
            project_id       STRING,
            project_name     STRING,
            environment      STRING,
            ref              STRING,
            sha              STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            status           STRING,
            creator          STRING,
            creator_email    STRING,
            description      STRING,
            duration_seconds INT,
            ingestion_timestamp TIMESTAMP,
            processing_date  DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Raw GitHub deployments enriched with Vyaguta project context'
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_deployment_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_deployment_bronze_staging AS src
        ON tgt.deployment_id = src.deployment_id AND tgt.processing_date = src.processing_date
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
        print(f"Deployment Bronze: no deployments for {processing_date}")
        return 0
    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"Deployment Bronze: merged {count} deployments for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Project-driven GitHub Deployment Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    ingest_bronze(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
