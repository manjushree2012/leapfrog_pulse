"""Bronze layer: raw GitHub commit ingestion into Delta table.

Responsibility: ingest source-like commit records with minimal transformation.
Only adds ingestion metadata (timestamp, date). No business logic.
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

from leapfrog_pulse.github.mock_source import get_commits_for_date

_RAW_SCHEMA = StructType(
    [
        StructField("commit_id", StringType(), False),
        StructField("organization", StringType(), False),
        StructField("repository", StringType(), False),
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


def build_bronze_df(spark: SparkSession, processing_date: str) -> DataFrame:
    """Fetch raw commits for processing_date and return a Bronze-schema DataFrame.

    This function is the boundary between the source and Bronze. Swap
    get_commits_for_date() here to plug in the real GitHub API.
    """
    raw = get_commits_for_date(processing_date)
    if not raw:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(raw, schema=_RAW_SCHEMA)
        .withColumn("committed_at", F.to_timestamp("committed_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("ingestion_date", F.lit(proc_date).cast(DateType()))
        .withColumn("processing_date", F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.github_bronze_commits"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            commit_id          STRING NOT NULL,
            organization       STRING NOT NULL,
            repository         STRING NOT NULL,
            author             STRING NOT NULL,
            author_email       STRING NOT NULL,
            committed_at       TIMESTAMP,
            message            STRING,
            branch             STRING,
            additions          INT,
            deletions          INT,
            changed_files      INT,
            ingestion_timestamp TIMESTAMP,
            ingestion_date     DATE,
            processing_date    DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    """Idempotent MERGE: upsert on commit_id so re-runs never duplicate records."""
    df.createOrReplaceTempView("_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _bronze_staging AS src
        ON tgt.commit_id = src.commit_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    """Resolve 'yesterday' to an actual ISO date; pass any other value through."""
    if processing_date == "yesterday":
        from datetime import timedelta

        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def ingest_bronze(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    """Full Bronze ingestion step. Returns number of records ingested."""
    processing_date = _resolve_date(processing_date)
    df = build_bronze_df(spark, processing_date)
    count = df.count()
    if count == 0:
        print(f"Bronze: no commits found for {processing_date}")
        return 0

    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"Bronze: merged {count} commits for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Bronze ingestion")
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

    ingest_bronze(spark, args.catalog, args.schema, processing_date)


if __name__ == "__main__":
    main()
