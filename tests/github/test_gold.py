"""Tests for Gold-layer aggregation logic.

Uses the spark fixture from conftest.py (requires Databricks Connect).
Tests focus on transform_gold() — the pure DataFrame → DataFrame function.
"""

from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from leapfrog_pulse.github.gold import transform_gold

_SILVER_SCHEMA = StructType(
    [
        StructField("commit_id", StringType(), False),
        StructField("organization", StringType(), False),
        StructField("repository", StringType(), False),
        StructField("author", StringType(), False),
        StructField("author_email", StringType(), True),
        StructField("committed_at", TimestampType(), False),
        StructField("commit_date", DateType(), False),
        StructField("branch", StringType(), True),
        StructField("additions", IntegerType(), True),
        StructField("deletions", IntegerType(), True),
        StructField("changed_files", IntegerType(), True),
        StructField("commit_message", StringType(), True),
        StructField("processing_date", DateType(), True),
    ]
)

_TS = datetime(2026, 9, 15, 10, 0, 0)
_DATE = date(2026, 9, 15)
_PROC = date(2026, 9, 15)


def _rows():
    return [
        ("c1", "leapfrog", "backend-api", "Aryan", "a@lf.com", _TS, _DATE, "main", 50, 10, 3, "msg", _PROC),
        ("c2", "leapfrog", "backend-api", "Aryan", "a@lf.com", _TS, _DATE, "main", 30, 5, 2, "msg", _PROC),
        ("c3", "leapfrog", "backend-api", "Priya", "p@lf.com", _TS, _DATE, "main", 20, 0, 1, "msg", _PROC),
        ("c4", "leapfrog", "frontend-web", "Aryan", "a@lf.com", _TS, _DATE, "main", 100, 50, 5, "msg", _PROC),
    ]


class TestTransformGold:
    def test_output_schema_has_required_columns(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        cols = set(result.columns)
        assert {"metric_date", "organization", "repository", "developer", "commit_count", "additions", "deletions", "changed_files", "updated_at"}.issubset(cols)

    def test_commit_count_per_developer_repo(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        aryan_backend = result.filter(
            (result.developer == "Aryan")
            & (result.repository == "backend-api")
        ).collect()
        assert len(aryan_backend) == 1
        assert aryan_backend[0]["commit_count"] == 2

    def test_additions_summed_per_developer_repo(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        row = result.filter(
            (result.developer == "Aryan") & (result.repository == "backend-api")
        ).collect()[0]
        assert row["additions"] == 80  # 50 + 30

    def test_deletions_summed_per_developer_repo(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        row = result.filter(
            (result.developer == "Aryan") & (result.repository == "backend-api")
        ).collect()[0]
        assert row["deletions"] == 15  # 10 + 5

    def test_changed_files_summed(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        row = result.filter(
            (result.developer == "Aryan") & (result.repository == "backend-api")
        ).collect()[0]
        assert row["changed_files"] == 5  # 3 + 2

    def test_separate_rows_for_different_repos(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        aryan_rows = result.filter(result.developer == "Aryan").collect()
        repos = {r["repository"] for r in aryan_rows}
        assert repos == {"backend-api", "frontend-web"}

    def test_separate_rows_for_different_developers(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        backend_rows = result.filter(result.repository == "backend-api").collect()
        developers = {r["developer"] for r in backend_rows}
        assert developers == {"Aryan", "Priya"}

    def test_metric_date_matches_commit_date(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        for row in result.collect():
            assert str(row["metric_date"]) == "2026-09-15"

    def test_total_output_rows(self, spark: SparkSession):
        # 4 commits across: Aryan/backend, Aryan/backend, Priya/backend, Aryan/frontend
        # → 3 distinct (dev, repo) groups
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        assert result.count() == 3

    def test_idempotent_aggregation(self, spark: SparkSession):
        df = spark.createDataFrame(_rows(), schema=_SILVER_SCHEMA)
        r1 = transform_gold(df).orderBy("developer", "repository").collect()
        r2 = transform_gold(df).orderBy("developer", "repository").collect()
        for a, b in zip(r1, r2):
            assert a["commit_count"] == b["commit_count"]
            assert a["additions"] == b["additions"]
            assert a["metric_date"] == b["metric_date"]

    def test_empty_input_returns_empty(self, spark: SparkSession):
        df = spark.createDataFrame([], schema=_SILVER_SCHEMA)
        result = transform_gold(df)
        assert result.count() == 0
