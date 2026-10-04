"""Google Calendar Gold layer — daily meeting-hours analytics.

Aggregates silver events into one row per processing_date capturing total
attendee-hours by meeting category. This feeds the time-allocation computation
in the dashboard gold mart.

Entry point: gcal_gold
"""

import argparse
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def transform_gold(df: DataFrame) -> DataFrame:
    """Aggregate silver events into daily meeting-hour metrics."""
    return (
        df.groupBy("processing_date")
        .agg(
            F.sum("attendee_hours").alias("total_attendee_hours"),
            F.sum(F.when(F.col("category") == "standup", F.col("attendee_hours")).otherwise(0)).alias("standup_hours"),
            F.sum(
                F.when(F.col("category") == "planning", F.col("attendee_hours")).otherwise(0)
            ).alias("planning_hours"),
            F.sum(
                F.when(F.col("category") == "retrospective", F.col("attendee_hours")).otherwise(0)
            ).alias("retro_hours"),
            F.sum(F.when(F.col("category") == "1on1", F.col("attendee_hours")).otherwise(0)).alias("oneon1_hours"),
            F.sum(
                F.when(F.col("category") == "code_review_meeting", F.col("attendee_hours")).otherwise(0)
            ).alias("code_review_meeting_hours"),
            F.sum(F.when(F.col("category") == "other", F.col("attendee_hours")).otherwise(0)).alias("other_meeting_hours"),
            F.count("event_id").alias("total_meetings"),
            F.lit(datetime.now(tz=timezone.utc)).cast("timestamp").alias("updated_at"),
        )
        .withColumn("metric_date", F.col("processing_date"))
        .drop("processing_date")
    )


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.gcal_gold_daily_meeting_hours"


def ensure_gold_table(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_table(catalog, schema)} (
            metric_date                    DATE    NOT NULL,
            total_attendee_hours           DOUBLE,
            standup_hours                  DOUBLE,
            planning_hours                 DOUBLE,
            retro_hours                    DOUBLE,
            oneon1_hours                   DOUBLE,
            code_review_meeting_hours      DOUBLE,
            other_meeting_hours            DOUBLE,
            total_meetings                 INT,
            updated_at                     TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (metric_date)
        COMMENT 'Daily Google Calendar meeting hours aggregated by category'
    """)


def merge_gold(spark: SparkSession, df: DataFrame, catalog: str, schema: str) -> None:
    df.createOrReplaceTempView("_gcal_gold_staging")
    spark.sql(f"""
        MERGE INTO {_table(catalog, schema)} AS tgt
        USING _gcal_gold_staging AS src
        ON tgt.metric_date = src.metric_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _resolve_date(processing_date: str) -> str:
    if processing_date == "yesterday":
        return (date.today() - timedelta(days=1)).isoformat()
    return processing_date


def process_gold(spark: SparkSession, catalog: str, schema: str, processing_date: str) -> int:
    processing_date = _resolve_date(processing_date)
    silver_df = spark.sql(f"""
        SELECT * FROM `{catalog}`.`{schema}`.gcal_silver_events
        WHERE processing_date = '{processing_date}'
    """)

    gold_df = transform_gold(silver_df)
    count = gold_df.count()
    if count == 0:
        print(f"GCal Gold: no records to process for {processing_date}")
        return 0

    ensure_gold_table(spark, catalog, schema)
    merge_gold(spark, gold_df, catalog, schema)
    print(f"GCal Gold: merged {count} metric rows for {processing_date} into {_table(catalog, schema)}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Calendar Gold processing")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--processing_date", required=True, help="YYYY-MM-DD or 'yesterday'")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    process_gold(spark, args.catalog, args.schema, args.processing_date)


if __name__ == "__main__":
    main()
