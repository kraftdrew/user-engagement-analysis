from pyspark.sql import functions as F
from src.engagement_analysis_processor import EngagementAnalysisProcessor
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import DataFrame


def _sample(spark: SparkSession) -> DataFrame:
    data = [
        (1, "2022-01-01 12:00:00", "home", 30),
        (1, "2022-01-01 12:15:00", "home", 20),
        (2, "2022-01-01 12:05:00", "dashboard", 45),
        (3, "2022-01-01 12:25:00", "dashboard", 40),
        (3, "2022-01-01 12:10:00", "profile", 60),
        (2, "2022-01-01 12:20:00", "profile", 30),
    ]

    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),  # ingest as string
        StructField("page", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
    ])

    return spark.createDataFrame(data, schema=schema)


def test_avg_duration_by_page(spark: SparkSession) -> None:
    df = _sample(spark)
    proc = EngagementAnalysisProcessor()
    got = {r["page"]: r["avg_duration_sec"] for r in proc.avg_duration_per_page(df).collect()}
    assert round(got["home"], 2) == 25.00
    assert round(got["dashboard"], 2) == 42.50
    assert round(got["profile"], 2) == 45.00


def test_most_engaging_page(spark: SparkSession) -> None:
    df = _sample(spark)
    proc = EngagementAnalysisProcessor()
    top = proc.most_engaging_page(df).collect()[0]
    assert top["page"] == "profile"
    assert round(top["avg_duration_sec"], 2) == 45.00