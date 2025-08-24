# tests/test_basic.py
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

PATTERN = "yyyy-MM-dd HH:mm:ss"
MIN_DT = "1900-01-01 00:00:00"
MAX_DT = "2999-12-31 23:59:59"

def _get_spark():
    s = SparkSession.getActiveSession()
    if s is None:
        # When run from notebook kernel, Spark must already exist.
        # If you hit this, run via the notebook (next step) or use a fixture in CI.
        raise RuntimeError("No active SparkSession. Run tests from a notebook kernel (see step 2).")
    return s

def _sample_df(spark):
    data = [
        (1, "2022-01-01 12:00:00", "home", 30),
        (2, "2022-01-01 12:05:00", "dashboard", 45),
        (3, "2022-01-01 12:10:00", "profile", 60),
        (1, "2022-01-01 12:15:00", "home", 20),
        (2, "2022-01-01 12:20:00", "profile", 30),
        (3, "2022-01-01 12:25:00", "dashboard", 40),
    ]
    return spark.createDataFrame(data, ["user_id","timestamp","page","duration_seconds"])

def test_not_nulls():
    spark = _get_spark()
    df = _sample_df(spark)
    assert df.filter(
        F.col("user_id").isNull() |
        F.col("timestamp").isNull() |
        F.col("page").isNull() |
        F.col("duration_seconds").isNull()
    ).count() == 0

def test_user_id_and_duration_min_values():
    spark = _get_spark()
    df = _sample_df(spark)
    assert df.filter(
        (F.col("user_id") < -1) | (F.col("duration_seconds") < -1)
    ).count() == 0

def test_timestamp_format_and_parseable():
    spark = _get_spark()
    df = _sample_df(spark)
    parsed = df.withColumn("ts_parsed", F.to_timestamp("timestamp", PATTERN))
    assert parsed.filter(F.col("ts_parsed").isNull()).count() == 0

def test_timestamp_in_range():
    spark = _get_spark()
    df = _sample_df(spark)
    parsed = df.withColumn("ts_parsed", F.to_timestamp("timestamp", PATTERN))
    assert parsed.filter(
        (F.col("ts_parsed") < F.to_timestamp(F.lit(MIN_DT))) |
        (F.col("ts_parsed") > F.to_timestamp(F.lit(MAX_DT)))
    ).count() == 0
