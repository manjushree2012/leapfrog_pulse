"""Google Calendar Silver layer — validated and normalized calendar events.

Reads from gcal_bronze_events, filters cancelled events and zero-duration events,
deduplicates on (event_id, processing_date), normalizes strings, and writes to
gcal_silver_events.

Entry point: gcal_silver
"""

import argparse
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform_silver(df: DataFrame) -> DataFrame:
    """Pure transformation from bronze to silver — no I/O."""
    df = df.filter(
        (F.col("status") != "cancelled")
        & (F.col("duration_hours") > 0)
        & (F.col("is_engineering_meeting") == True)
    )

    df = (
        df.withColumn("title", F.trim("title"))
        .withColumn("organizer_email", F.trim("organizer_email"))
        .withColumn("category", F.trim("category"))
    )

    # Deduplicate: keep the record with the latest start_at per (event_id, processing_date)
    window = Window.partitionBy("event_id", "processing_date").orderBy(F.col("start_at").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    processed_at = datetime.now(timezone.utc)
    df = df.withColumn("processed_at", F.lit(processed_at).cast("timestamp"))

    return df.select(
        "event_id",
        "title",
        "start_at",
        "end_at",
        "duration_hours",
        "organizer_email",
        "attendee_emails",
        "attendee_count",
        "accepted_count",
        "status",
        "is_recurring",
        "category",
        "is_engineering_meeting",
        "attendee_hours",
        "processing_date",
        "processed_at",
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.gcal_silver_events"


def ensure_silver_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            event_id                   STRING  NOT NULL,
            title                      STRING,
            start_at                   TIMESTAMP,
            end_at                     TIMESTAMP,
            duration_hours             DOUBLE,
            organizer_email            STRING,
            attendee_emails            STRING,
            attendee_count             INT,
            accepted_count             INT,
            status                     STRING,
            is_recurring               BOOLEAN,
            category                   STRING,
            is_engineering_meeting     BOOLEAN,
            attendee_hours             DOUBLE,
            processing_date            DATE,
            processed_at               TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Google Calendar events — validated and normalized, engineering meetings only'
    """)


def merge_silver(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_gcal_silver_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _gcal_silver_staging AS src
        ON tgt.event_id = src.event_id AND tgt.processing_date = src.processing_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_silver(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    bronze_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.gcal_bronze_events
        WHERE processing_date = '{processing_date}'
    """)

    silver_df = transform_silver(bronze_df)
    count = silver_df.count()
    if count == 0:
        print(f"GCal Silver: no records to process for {processing_date}")
        return 0

    ensure_silver_table(spark, catalog, schema)
    merge_silver(spark, silver_df, catalog, schema)
    print(f"GCal Silver: merged {count} events for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Calendar Silver processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_silver(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
