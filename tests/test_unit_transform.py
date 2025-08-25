from pyspark.sql import functions as F
from src.engagement_analysis_processor import EngagementAnalysisProcessor


def _sample(spark):
    data = [
        (1, "2022-01-01 12:00:00", "home",      30),
        (1, "2022-01-01 12:15:00", "home",      20),
        (2, "2022-01-01 12:05:00", "dashboard", 45),
        (3, "2022-01-01 12:25:00", "dashboard", 40),
        (3, "2022-01-01 12:10:00", "profile",   60),
        (2, "2022-01-01 12:20:00", "profile",   30),
    ]
    schema = "user_id INT, timestamp STRING, page STRING, duration_seconds INT"
    return spark.createDataFrame(data, schema=schema)


def test_avg_duration_by_page(spark):
    df = _sample(spark)
    processor = EngagementAnalysisProcessor(df)
    got = {
        r["page"]: r["avg_duration_sec"]
        for r in processor.avg_duration_per_page().collect()
    }
    print(got)
    assert round(got["home"], 2) == 25.00
    assert round(got["dashboard"], 2) == 42.50
    assert round(got["profile"], 2) == 45.00


def test_most_engaging_page(spark):
    df = _sample(spark)
    processor = EngagementAnalysisProcessor(df)
    top = processor.most_engaging_page().collect()[0]
    assert top["page"] == "profile"
    assert round(top["avg_duration_sec"], 2) == 45.00
