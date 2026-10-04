"""Bronze layer: project-driven GitHub pull request ingestion.

Reads the active project-to-repository mapping from the Vyaguta bronze table,
then fetches pull requests created/updated on processing_date for each repo.
Writes to github_bronze_project_prs with project context (project_id, project_name).

Entry point: project_github_pr_bronze
"""

import argparse
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from leapfrog_pulse.github.mock_source import get_pull_requests_for_repo_and_date

_RAW_SCHEMA = StructType([
    StructField("pr_id",                     StringType(),  False),
    StructField("number",                    IntegerType(), True),
    StructField("title",                     StringType(),  True),
    StructField("state",                     StringType(),  True),
    StructField("repository",                StringType(),  False),
    StructField("organization",              StringType(),  False),
    StructField("project_id",               StringType(),  True),
    StructField("project_name",             StringType(),  True),
    StructField("author",                    StringType(),  True),
    StructField("author_email",              StringType(),  True),
    StructField("created_at",               StringType(),  True),
    StructField("updated_at",               StringType(),  True),
    StructField("merged_at",                StringType(),  True),
    StructField("closed_at",                StringType(),  True),
    StructField("first_review_submitted_at", StringType(),  True),
    StructField("first_reviewer",           StringType(),  True),
    StructField("base_ref",                 StringType(),  True),
    StructField("head_ref",                 StringType(),  True),
    StructField("additions",                IntegerType(), True),
    StructField("deletions",                IntegerType(), True),
    StructField("changed_files",            IntegerType(), True),
    StructField("is_merged",                BooleanType(), True),
    StructField("draft",                    BooleanType(), True),
    StructField("ci_checks_total",           IntegerType(), True),
    StructField("ci_checks_failed",          IntegerType(), True),
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

    all_prs: list[dict] = []
    for row in repos:
        org, repo_name = _parse_repo_url(row["github_repo_url"])
        prs = get_pull_requests_for_repo_and_date(repo_name, org, processing_date)
        for pr in prs:
            pr["project_id"] = row["project_id"]
            pr["project_name"] = row["project_name"]
            all_prs.append(pr)

    if not all_prs:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(all_prs, schema=_RAW_SCHEMA)
        .withColumn("created_at",                F.to_timestamp("created_at"))
        .withColumn("updated_at",                F.to_timestamp("updated_at"))
        .withColumn("merged_at",                 F.to_timestamp("merged_at"))
        .withColumn("closed_at",                 F.to_timestamp("closed_at"))
        .withColumn("first_review_submitted_at", F.to_timestamp("first_review_submitted_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("processing_date",     F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_bronze_project_prs"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            pr_id                     STRING NOT NULL,
            number                    INT,
            title                     STRING,
            state                     STRING,
            repository                STRING NOT NULL,
            organization              STRING NOT NULL,
            project_id                STRING,
            project_name              STRING,
            author                    STRING,
            author_email              STRING,
            created_at                TIMESTAMP,
            updated_at                TIMESTAMP,
            merged_at                 TIMESTAMP,
            closed_at                 TIMESTAMP,
            first_review_submitted_at TIMESTAMP,
            first_reviewer            STRING,
            base_ref                  STRING,
            head_ref                  STRING,
            additions                 INT,
            deletions                 INT,
            changed_files             INT,
            is_merged                 BOOLEAN,
            draft                     BOOLEAN,
            ci_checks_total           INT,
            ci_checks_failed          INT,
            ingestion_timestamp       TIMESTAMP,
            processing_date           DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Raw GitHub pull requests enriched with Vyaguta project context'
    """)
    table_name = _table(catalog, schema)
    existing_columns = {field.name.lower() for field in spark.table(table_name).schema.fields}
    missing_columns = [
        f"{name} INT"
        for name in ("ci_checks_total", "ci_checks_failed")
        if name not in existing_columns
    ]
    if missing_columns:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(missing_columns)})")


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_pr_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_pr_bronze_staging AS src
        ON tgt.pr_id = src.pr_id AND tgt.processing_date = src.processing_date
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
        print(f"PR Bronze: no PRs found for {processing_date}")
        return 0
    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"PR Bronze: merged {count} PRs for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Project-driven GitHub PR Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    ingest_bronze(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
