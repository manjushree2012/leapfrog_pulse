"""Bronze layer: project-driven GitHub commit ingestion.

Reads the active project-to-repository mapping from the Vyaguta bronze table,
then fetches commits for each repository on the given processing date.
Writes to github_bronze_project_commits, which includes project context
(project_id, project_name) alongside each commit record.

Entry point: project_github_bronze
"""

import argparse
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

from leapfrog_pulse.github.mock_source import get_commits_for_repo_and_date

_RAW_SCHEMA = StructType(
    [
        StructField("commit_id", StringType(), False),
        StructField("organization", StringType(), False),
        StructField("repository", StringType(), False),
        StructField("project_id", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("author", StringType(), False),
        StructField("author_email", StringType(), False),
        StructField("committed_at", StringType(), True),
        StructField("message", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("additions", IntegerType(), True),
        StructField("deletions", IntegerType(), True),
        StructField("changed_files", IntegerType(), True),
    ]
)


def _parse_repo_url(url: str) -> tuple[str, str]:
    """Extract (org, repo_name) from a GitHub URL like https://github.com/org/repo."""
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

    all_commits: list[dict] = []
    for row in repos:
        org, repo_name = _parse_repo_url(row["github_repo_url"])
        commits = get_commits_for_repo_and_date(repo_name, org, processing_date)
        for c in commits:
            c["project_id"] = row["project_id"]
            c["project_name"] = row["project_name"]
            all_commits.append(c)

    if not all_commits:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(all_commits, schema=_RAW_SCHEMA)
        .withColumn("committed_at", F.to_timestamp("committed_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("ingestion_date", F.lit(proc_date).cast(DateType()))
        .withColumn("processing_date", F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_bronze_project_commits"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            commit_id           STRING NOT NULL,
            organization        STRING NOT NULL,
            repository          STRING NOT NULL,
            project_id          STRING,
            project_name        STRING,
            author              STRING NOT NULL,
            author_email        STRING NOT NULL,
            committed_at        TIMESTAMP,
            message             STRING,
            branch              STRING,
            additions           INT,
            deletions           INT,
            changed_files       INT,
            ingestion_timestamp TIMESTAMP,
            ingestion_date      DATE,
            processing_date     DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Raw GitHub commits enriched with Vyaguta project context'
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_project_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_project_bronze_staging AS src
        ON tgt.commit_id = src.commit_id
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
        print(f"Project GitHub Bronze: no commits found for {processing_date}")
        return 0
    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"Project GitHub Bronze: merged {count} commits for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Project-driven GitHub Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    ingest_bronze(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
