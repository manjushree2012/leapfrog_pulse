"""Silver layer: cleaned and normalized GitHub commits.

Responsibilities:
- Cast committed_at to TIMESTAMP and extract commit_date as DATE
- Normalize string fields (trim whitespace, lowercase emails)
- Drop records with null commit_id, repository, organization, or committed_at
- Validate additions/deletions are non-negative; coerce negatives to 0
- Deduplicate by commit_id (keep latest processing_date on conflict)
- Add processed_at metadata timestamp
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform_silver(df: DataFrame) -> DataFrame:
    """Apply Silver-layer cleaning rules to a Bronze DataFrame.

    Pure transformation: no I/O, fully testable without a Delta table.
    """
    # Data quality: drop records missing required fields
    df = df.filter(
        F.col("commit_id").isNotNull()
        & F.col("organization").isNotNull()
        & F.col("repository").isNotNull()
        & F.col("committed_at").isNotNull()
    )

    # Normalize strings
    df = df.withColumn("author", F.trim("author")).withColumn("author_email", F.lower(F.trim("author_email")))

    # Coerce negative line counts to 0
    df = df.withColumn("additions", F.greatest(F.col("additions"), F.lit(0))).withColumn(
        "deletions", F.greatest(F.col("deletions"), F.lit(0))
    )

    # Extract commit_date from committed_at
    df = df.withColumn("commit_date", F.to_date("committed_at"))

    # Rename message → commit_message for clarity
    df = df.withColumnRenamed("message", "commit_message")

    # Deduplicate: one row per commit_id, prefer the latest processing_date
    window = Window.partitionBy("commit_id").orderBy(F.col("processing_date").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    # Add processing metadata
    processed_at = datetime.now(timezone.utc)
    df = df.withColumn("processed_at", F.lit(processed_at).cast("timestamp"))

    return df.select(
        "commit_id",
        "organization",
        "repository",
        "author",
        "author_email",
        "committed_at",
        "commit_date",
        "branch",
        "additions",
        "deletions",
        "changed_files",
        "commit_message",
        "processing_date",
        "processed_at",
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_silver_commits"


def _bronze_table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_bronze_commits"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            commit_id      STRING NOT NULL,
            organization   STRING NOT NULL,
            repository     STRING NOT NULL,
            author         STRING NOT NULL,
            author_email   STRING NOT NULL,
            committed_at   TIMESTAMP NOT NULL,
            commit_date    DATE NOT NULL,
            branch         STRING,
            additions      INT,
            deletions      INT,
            changed_files  INT,
            commit_message STRING,
            processing_date DATE,
            processed_at   TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (commit_date)
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    """Idempotent MERGE: upsert on commit_id so re-runs never duplicate records."""
    df.createOrReplaceTempView("_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _silver_staging AS src
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
    """Full Silver processing step for processing_date. Returns output record count."""
    processing_date = _resolve_date(processing_date)
    bronze_df = spark.sql(f"""
        SELECT * FROM {_bronze_table(catalog, schema)}
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"Silver: no records to process for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"Silver: merged {count} commits for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    if args.processing_date == "yesterday":
        from datetime import date, timedelta

        processing_date = (date.today() - timedelta(days=1)).isoformat()
    else:
        processing_date = args.processing_date

    process_silver(spark, args.catalog, args.schema, processing_date)


if __name__ == "__main__":
    main()
