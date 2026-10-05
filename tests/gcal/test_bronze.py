from leapfrog_pulse.gcal import bronze


class _Rows:
    def collect(self):
        return [{"employee_email": "member@example.com"}]


class _DataFrame:
    def withColumn(self, *_args):
        return self


class _Spark:
    def __init__(self):
        self.queries = []
        self.created_rows = None

    def sql(self, query):
        self.queries.append(query)
        return _Rows()

    def createDataFrame(self, rows, schema):
        self.created_rows = rows
        return _DataFrame()


def test_build_bronze_df_uses_vyaguta_member_emails(monkeypatch):
    spark = _Spark()
    source_calls = []

    def get_events(processing_date, engineer_emails):
        source_calls.append((processing_date, engineer_emails))
        return [{"event_id": "event-1"}]

    monkeypatch.setattr(bronze, "get_events_for_date", get_events)

    bronze.build_bronze_df(spark, "dev", "test", "2026-10-05")

    assert source_calls == [("2026-10-05", ["member@example.com"])]
    assert "vyaguta_bronze_project_members" in spark.queries[0]
    assert spark.created_rows == [{"event_id": "event-1"}]
