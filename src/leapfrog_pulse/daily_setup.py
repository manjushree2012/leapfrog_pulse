"""Daily ingest pipeline — table setup.

Creates all Delta tables required by the daily_ingest_pipeline job if they do
not yet exist. Running this as the first task guarantees that downstream tasks
can both write to and read from these tables without race conditions.

Entry point: daily_ingest_setup
"""

import argparse

from pyspark.sql import SparkSession

from leapfrog_pulse.gcal.bronze import ensure_bronze_table as ensure_gcal_bronze
from leapfrog_pulse.gcal.gold import ensure_gold_table as ensure_gcal_gold
from leapfrog_pulse.gcal.silver import ensure_silver_table as ensure_gcal_silver
from leapfrog_pulse.github.project_bronze import ensure_bronze_table as ensure_github_project_bronze
from leapfrog_pulse.github.project_gold import ensure_gold_table as ensure_github_project_gold
from leapfrog_pulse.github.project_silver import ensure_silver_table as ensure_github_project_silver
from leapfrog_pulse.gold.dashboard import (
    ensure_activity_table,
    ensure_kpis_table,
    ensure_project_summary_table,
    ensure_time_allocation_table,
)
from leapfrog_pulse.jira.bronze import ensure_bronze_table as ensure_jira_bronze
from leapfrog_pulse.jira.gold import ensure_gold_table as ensure_jira_gold
from leapfrog_pulse.jira.silver import ensure_silver_table as ensure_jira_silver
from leapfrog_pulse.vyaguta.bronze import ensure_bronze_table as ensure_vyaguta_bronze


def setup_all_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    # Bronze
    ensure_vyaguta_bronze(spark, catalog, schema)
    print(f"Setup: vyaguta_bronze_projects ready in {catalog}.{schema}")
    ensure_github_project_bronze(spark, catalog, schema)
    print(f"Setup: github_bronze_project_commits ready in {catalog}.{schema}")
    ensure_jira_bronze(spark, catalog, schema)
    print(f"Setup: jira_bronze_issues ready in {catalog}.{schema}")
    ensure_gcal_bronze(spark, catalog, schema)
    print(f"Setup: gcal_bronze_events ready in {catalog}.{schema}")

    # Silver
    ensure_github_project_silver(spark, catalog, schema)
    print(f"Setup: github_silver_project_commits ready in {catalog}.{schema}")
    ensure_jira_silver(spark, catalog, schema)
    print(f"Setup: jira_silver_issues ready in {catalog}.{schema}")
    ensure_gcal_silver(spark, catalog, schema)
    print(f"Setup: gcal_silver_events ready in {catalog}.{schema}")

    # Gold
    ensure_github_project_gold(spark, catalog, schema)
    print(f"Setup: github_gold_project_daily_metrics ready in {catalog}.{schema}")
    ensure_jira_gold(spark, catalog, schema)
    print(f"Setup: jira_gold_project_daily_metrics ready in {catalog}.{schema}")
    ensure_gcal_gold(spark, catalog, schema)
    print(f"Setup: gcal_gold_daily_meeting_hours ready in {catalog}.{schema}")

    # Dashboard mart
    ensure_project_summary_table(spark, catalog, schema)
    ensure_kpis_table(spark, catalog, schema)
    ensure_activity_table(spark, catalog, schema)
    ensure_time_allocation_table(spark, catalog, schema)
    print(f"Setup: dashboard gold mart tables ready in {catalog}.{schema}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily ingest pipeline — table setup")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_known_args()[0]

    from databricks.sdk.runtime import spark  # noqa: PLC0415

    spark.sql(f"USE CATALOG `{args.catalog}`")
    setup_all_tables(spark, args.catalog, args.schema)
    print("Setup complete — all tables exist.")


if __name__ == "__main__":
    main()
