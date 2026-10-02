"""Silver layer: GitHub pull request validation and enrichment.

Reads from github_bronze_project_prs, validates timestamps, deduplicates,
and computes review_time_seconds for PRs that have a first review timestamp.

Entry point: project_github_pr_silver
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform_silver(df: DataFrame) -> DataFrame:
    # Drop rows missing required keys
    df = df.filter(
        F.col("pr_id").isNotNull()
        & F.col("organization").isNotNull()
        & F.col("repository").isNotNull()
        & F.col("created_at").isNotNull()
    )

    # Trim strings
    df = (
        df.withColumn("author",        F.trim("author"))
          .withColumn("author_email",   F.lower(F.trim("author_email")))
          .withColumn("first_reviewer", F.trim("first_reviewer"))
    )

    # Coerce negative additions/deletions
    df = (
        df.withColumn("additions",    F.greatest(F.col("additions"),    F.lit(0)))
          .withColumn("deletions",    F.greatest(F.col("deletions"),    F.lit(0)))
          .withColumn("changed_files", F.greatest(F.col("changed_files"), F.lit(0)))
    )

    # Compute review_time_seconds (null when no review yet)
    df = df.withColumn(
        "review_time_seconds",
        F.when(
            F.col("first_review_submitted_at").isNotNull() & F.col("created_at").isNotNull(),
            (F.unix_timestamp("first_review_submitted_at") - F.unix_timestamp("created_at")),
        ).otherwise(F.lit(None).cast("long")),
    )

    # Derive pr_date from created_at
    df = df.withColumn("pr_date", F.to_date("created_at"))

    # Deduplicate: keep most recent processing_date per pr_id
    w = Window.partitionBy("pr_id").orderBy(F.col("processing_date").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    return df.withColumn("processed_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_silver_project_prs"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
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
            review_time_seconds       LONG,
            pr_date                   DATE,
            processing_date           DATE,
            processed_at              TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (pr_date)
        COMMENT 'Validated GitHub pull requests with computed review_time_seconds'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_github_pr_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _github_pr_silver_staging AS src
        ON tgt.pr_id = src.pr_id
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
        SELECT * FROM `{catalog}`.`{schema}`.github_bronze_project_prs
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"PR Silver: no records for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"PR Silver: merged {count} PRs for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub PR Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_silver(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
