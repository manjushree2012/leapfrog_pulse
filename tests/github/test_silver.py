"""Tests for Silver-layer transformation logic.

Uses the spark fixture from conftest.py (requires Databricks Connect).
Tests focus on transform_silver() — the pure DataFrame → DataFrame function.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from leapfrog_pulse.github.silver import transform_silver

_BRONZE_SCHEMA = StructType(
    [
        StructField("commit_id", StringType(), True),
        StructField("organization", StringType(), True),
        StructField("repository", StringType(), True),
        StructField("author", StringType(), True),
        StructField("author_email", StringType(), True),
        StructField("committed_at", TimestampType(), True),
        StructField("message", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("additions", IntegerType(), True),
        StructField("deletions", IntegerType(), True),
        StructField("changed_files", IntegerType(), True),
        StructField("processing_date", DateType(), True),
    ]
)

_TS = datetime(2026, 9, 15, 10, 30, 0)
_PROC_DATE = datetime(2026, 9, 15).date()


def _base_rows():
    return [
        ("abc123", "leapfrog", "backend-api", "Aryan Sharma", "aryan@lf.com", _TS, "Add auth", "main", 50, 10, 3, _PROC_DATE),
        ("def456", "leapfrog", "frontend-web", "Priya Patel", "priya@lf.com", _TS, "Fix bug", "main", 20, 5, 2, _PROC_DATE),
    ]


class TestTransformSilver:
    def test_basic_output_schema(self, spark: SparkSession):
        df = spark.createDataFrame(_base_rows(), schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        cols = set(result.columns)
        assert "commit_id" in cols
        assert "commit_date" in cols
        assert "commit_message" in cols
        assert "message" not in cols, "message should be renamed to commit_message"
        assert "processed_at" in cols

    def test_deduplication_keeps_one_row_per_commit_id(self, spark: SparkSession):
        rows = _base_rows() + [
            ("abc123", "leapfrog", "backend-api", "Aryan Sharma", "aryan@lf.com", _TS, "Dup commit", "main", 50, 10, 3, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        assert result.filter("commit_id = 'abc123'").count() == 1

    def test_null_commit_id_dropped(self, spark: SparkSession):
        rows = _base_rows() + [
            (None, "leapfrog", "backend-api", "Aryan", "a@lf.com", _TS, "msg", "main", 1, 0, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        assert result.filter("commit_id is null").count() == 0

    def test_null_organization_dropped(self, spark: SparkSession):
        rows = _base_rows() + [
            ("xyz999", None, "backend-api", "Aryan", "a@lf.com", _TS, "msg", "main", 1, 0, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        assert result.filter("organization is null").count() == 0

    def test_null_committed_at_dropped(self, spark: SparkSession):
        rows = _base_rows() + [
            ("nots000", "leapfrog", "backend-api", "Aryan", "a@lf.com", None, "msg", "main", 1, 0, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        assert result.filter("committed_at is null").count() == 0

    def test_negative_additions_coerced_to_zero(self, spark: SparkSession):
        rows = [
            ("neg001", "leapfrog", "backend-api", "Dev", "dev@lf.com", _TS, "msg", "main", -5, 10, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        row = result.filter("commit_id = 'neg001'").collect()[0]
        assert row["additions"] == 0

    def test_negative_deletions_coerced_to_zero(self, spark: SparkSession):
        rows = [
            ("neg002", "leapfrog", "backend-api", "Dev", "dev@lf.com", _TS, "msg", "main", 10, -3, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        row = result.filter("commit_id = 'neg002'").collect()[0]
        assert row["deletions"] == 0

    def test_author_email_lowercased(self, spark: SparkSession):
        rows = [
            ("case001", "leapfrog", "backend-api", "Dev", "DEV@LF.COM", _TS, "msg", "main", 1, 0, 1, _PROC_DATE),
        ]
        df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        row = result.filter("commit_id = 'case001'").collect()[0]
        assert row["author_email"] == "dev@lf.com"

    def test_commit_date_extracted_from_committed_at(self, spark: SparkSession):
        df = spark.createDataFrame(_base_rows(), schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        row = result.filter("commit_id = 'abc123'").collect()[0]
        assert str(row["commit_date"]) == "2026-09-15"

    def test_record_count_after_clean_input(self, spark: SparkSession):
        df = spark.createDataFrame(_base_rows(), schema=_BRONZE_SCHEMA)
        result = transform_silver(df)
        assert result.count() == 2

    def test_idempotent_transform(self, spark: SparkSession):
        df = spark.createDataFrame(_base_rows(), schema=_BRONZE_SCHEMA)
        result_a = transform_silver(df).orderBy("commit_id").collect()
        result_b = transform_silver(df).orderBy("commit_id").collect()
        # Exclude processed_at (timestamp differs per call); check stable fields
        for a, b in zip(result_a, result_b):
            assert a["commit_id"] == b["commit_id"]
            assert a["additions"] == b["additions"]
            assert a["commit_date"] == b["commit_date"]
