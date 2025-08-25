"""Contract/Data-Quality tests using StructType schema."""

import os
import sys
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

PATTERN = "yyyy-MM-dd HH:mm:ss"

EXPECTED_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),  # ingest as string
        StructField("page", StringType(), nullable=False),
        StructField("duration_seconds", IntegerType(), nullable=False),
    ]
)

ALLOWED_PAGES = {"home", "dashboard", "profile"}


def _valid_df(spark: SparkSession) -> DataFrame:
    """Small valid sample matching the ingest contract."""
    data: List[Tuple[int, str, str, int]] = [
        (1, "2022-01-01 12:00:00", "home", 30),
        (2, "2022-01-01 12:05:00", "dashboard", 45),
        (3, "2022-01-01 12:10:00", "profile", 60),
    ]
    return spark.createDataFrame(data=data, schema=EXPECTED_SCHEMA)


def test_schema_exact(spark: SparkSession) -> None:
    """Schema must match exactly (names, order, types, nullability)."""
    df = _valid_df(spark)
    assert df.schema == EXPECTED_SCHEMA


def test_no_extra_columns(spark: SparkSession) -> None:
    """Detect unexpected upstream columns."""
    df = _valid_df(spark).withColumn("referrer", F.lit("google"))
    expected_names = [f.name for f in EXPECTED_SCHEMA.fields]
    extras = set(df.columns) - set(expected_names)
    assert extras == {"referrer"}


def test_required_not_null_and_ranges_and_domain(spark: SparkSession) -> None:
    """Nulls, non-negative numeric ranges, and enum set for page."""
    df = _valid_df(spark)

    # not null
    assert (
        df.filter(
            F.col("user_id").isNull()
            | F.col("timestamp").isNull()
            | F.col("page").isNull()
            | F.col("duration_seconds").isNull()
        ).count()
        == 0
    )

    # ranges
    assert (
        df.filter((F.col("user_id") < 0) | (F.col("duration_seconds") < 0)).count()
        == 0
    )

    # enum domain for page
    assert df.filter(~F.col("page").isin(*ALLOWED_PAGES)).count() == 0


def test_timestamp_parseable(spark: SparkSession) -> None:
    """String timestamps must be parseable with the agreed pattern."""
    df = _valid_df(spark)
    parsed = df.withColumn("ts", F.to_timestamp("timestamp", PATTERN))
    assert parsed.filter(F.col("ts").isNull()).count() == 0
