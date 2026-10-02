"""Daily ingest pipeline — table setup.

Creates all Delta tables required by the daily_ingest_pipeline job if they do
not yet exist. Running this as the first task guarantees that downstream tasks
can both write to and read from these tables without race conditions.

Entry point: daily_ingest_setup
"""

import argparse

from pyspark.sql import SparkSession

from leapfrog_pulse.github.project_bronze import ensure_bronze_table as ensure_github_project_bronze
from leapfrog_pulse.jira.bronze import ensure_bronze_table as ensure_jira_bronze
from leapfrog_pulse.vyaguta.bronze import ensure_bronze_table as ensure_vyaguta_bronze


def setup_all_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    ensure_vyaguta_bronze(spark, catalog, schema)
    print(f"Setup: vyaguta_bronze_projects ready in {catalog}.{schema}")
    ensure_github_project_bronze(spark, catalog, schema)
    print(f"Setup: github_bronze_project_commits ready in {catalog}.{schema}")
    ensure_jira_bronze(spark, catalog, schema)
    print(f"Setup: jira_bronze_issues ready in {catalog}.{schema}")


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
