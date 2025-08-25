from pyspark.sql import functions as F
from src.engagement import avg_duration_per_page, most_engaging_page

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
    got = {r["page"]: r["avg_duration_sec"] for r in avg_duration_per_page(df).collect()}
    print (got)
    assert round(got["home"], 2) == 25.00
    assert round(got["dashboard"], 2) == 42.50
    assert round(got["profile"], 2) == 45.00

def test_most_engaging_page(spark):
    df = _sample(spark)
    top = most_engaging_page(df).collect()[0]
    assert top["page"] == "profile"
    assert round(top["avg_duration_sec"], 2) == 45.00
