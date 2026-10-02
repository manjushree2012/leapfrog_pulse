"""GitHub project-driven Silver layer.

Reads from github_bronze_project_commits, applies validation and normalization,
and writes to github_silver_project_commits. Includes project context columns
(project_id, project_name) inherited from the Vyaguta-driven bronze ingestion.

Validation rules:
  - Drop rows missing commit_id, organization, repository, committed_at, or project_id
  - Trim whitespace; lowercase author_email, organization, repository
  - Coerce negative additions / deletions / changed_files to 0
  - Deduplicate by commit_id (latest processing_date wins)
  - Extract commit_date (DATE) and commit_hour (INT) from committed_at

Entry point: project_github_silver
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform_silver(df: DataFrame) -> DataFrame:
    """Pure transformation from bronze to silver — no I/O."""
    df = df.filter(
        F.col("commit_id").isNotNull()
        & F.col("organization").isNotNull()
        & F.col("repository").isNotNull()
        & F.col("committed_at").isNotNull()
        & F.col("project_id").isNotNull()
        & F.col("author").isNotNull()
        & F.col("author_email").isNotNull()
    )

    df = (
        df.withColumn("author", F.trim("author"))
        .withColumn("author_email", F.lower(F.trim("author_email")))
        .withColumn("organization", F.lower(F.trim("organization")))
        .withColumn("repository", F.lower(F.trim("repository")))
        .withColumn("project_name", F.trim("project_name"))
        .withColumn("commit_message", F.trim(F.col("message")))
        .drop("message")
    )

    df = (
        df.withColumn("additions", F.greatest(F.coalesce(F.col("additions"), F.lit(0)), F.lit(0)))
        .withColumn("deletions", F.greatest(F.coalesce(F.col("deletions"), F.lit(0)), F.lit(0)))
        .withColumn("changed_files", F.greatest(F.coalesce(F.col("changed_files"), F.lit(0)), F.lit(0)))
    )

    df = (
        df.withColumn("commit_date", F.to_date("committed_at"))
        .withColumn("commit_hour", F.hour("committed_at"))
    )

    window = Window.partitionBy("commit_id").orderBy(F.col("processing_date").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    processed_at = datetime.now(timezone.utc)
    df = df.withColumn("processed_at", F.lit(processed_at).cast("timestamp"))

    return df.select(
        "commit_id",
        "organization",
        "repository",
        "project_id",
        "project_name",
        "author",
        "author_email",
        "committed_at",
        "commit_date",
        "commit_hour",
        "branch",
        "additions",
        "deletions",
        "changed_files",
        "commit_message",
        "processing_date",
        "processed_at",
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_silver_project_commits"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            commit_id       STRING  NOT NULL,
            organization    STRING  NOT NULL,
            repository      STRING  NOT NULL,
            project_id      STRING  NOT NULL,
            project_name    STRING,
            author          STRING  NOT NULL,
            author_email    STRING  NOT NULL,
            committed_at    TIMESTAMP NOT NULL,
            commit_date     DATE    NOT NULL,
            commit_hour     INT,
            branch          STRING,
            additions       INT,
            deletions       INT,
            changed_files   INT,
            commit_message  STRING,
            processing_date DATE,
            processed_at    TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (commit_date)
        COMMENT 'GitHub commits cleaned and enriched with Vyaguta project context'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_project_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_project_silver_staging AS src
        ON tgt.commit_id = src.commit_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        from datetime import date, timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_silver(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    bronze_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.github_bronze_project_commits
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"Project GitHub Silver: no records to process for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"Project GitHub Silver: merged {count} commits for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Project GitHub Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_silver(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
