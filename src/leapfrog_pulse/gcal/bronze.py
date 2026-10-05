"""Google Calendar Bronze layer — raw ingestion of calendar events for a given day.

Fetches all calendar events on processing_date and writes to gcal_bronze_events.
Delta table partitioned by processing_date. Primary key: (event_id, processing_date).

Entry point: gcal_bronze
"""

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from leapfrog_pulse.gcal.mock_source import get_events_for_date

_RAW_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("start_at", StringType(), True),
        StructField("end_at", StringType(), True),
        StructField("duration_hours", DoubleType(), True),
        StructField("organizer_email", StringType(), True),
        StructField("attendee_emails", StringType(), True),
        StructField("attendee_count", IntegerType(), True),
        StructField("accepted_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("is_recurring", BooleanType(), True),
        StructField("category", StringType(), True),
        StructField("is_engineering_meeting", BooleanType(), True),
        StructField("attendee_hours", DoubleType(), True),
    ]
)


def build_bronze_df(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> DataFrame:
    members = spark.sql(f"""
        SELECT DISTINCT LOWER(TRIM(employee_email)) AS employee_email
        FROM `{catalog}`.`{schema}`.`vyaguta_bronze_project_members`
        WHERE employee_email IS NOT NULL AND TRIM(employee_email) != ''
    """).collect()
    engineer_emails = [member["employee_email"] for member in members]
    events = get_events_for_date(processing_date, engineer_emails)

    if not events:
        return spark.createDataFrame([], _RAW_SCHEMA)

    ingestion_ts = datetime.now(timezone.utc)
    proc_date = date.fromisoformat(processing_date)

    return (
        spark.createDataFrame(events, schema=_RAW_SCHEMA)
        .withColumn("start_at", F.to_timestamp("start_at"))
        .withColumn("end_at", F.to_timestamp("end_at"))
        .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("processing_date", F.lit(proc_date).cast(DateType()))
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.gcal_bronze_events"


def ensure_bronze_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            event_id                   STRING NOT NULL,
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
            ingestion_timestamp        TIMESTAMP,
            processing_date            DATE
        )
        USING DELTA
        PARTITIONED BY (processing_date)
        COMMENT 'Raw Google Calendar events per day, ingested from calendar API'
    """)


def merge_bronze(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    """Idempotent MERGE: upsert on (event_id, processing_date) so re-runs never duplicate."""
    df.createOrReplaceTempView("_gcal_bronze_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _gcal_bronze_staging AS src
        ON tgt.event_id = src.event_id AND tgt.processing_date = src.processing_date
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
        print(f"GCal Bronze: no events found for {processing_date}")
        return 0
    ensure_bronze_table(spark, catalog, schema)
    merge_bronze(spark, df, catalog, schema)
    print(f"GCal Bronze: merged {count} events for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Calendar Bronze ingestion")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    ingest_bronze(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
