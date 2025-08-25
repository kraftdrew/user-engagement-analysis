"""Contract/Data-Quality tests using StructType schema."""

import os
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


class ContractDQ:
    def __init__(self, df: DataFrame):
        self.df = df
        self.invalid_df = df

    def test_schema_exact(self):
        """Schema must match exactly (names, order, types, nullability)."""
        if self.df.schema != EXPECTED_SCHEMA:
            self.invalid_df = self.invalid_df.withColumn("invalid_schema", F.lit(True))

    def test_no_extra_columns(self):
        """Detect unexpected upstream columns."""
        expected_names = [f.name for f in EXPECTED_SCHEMA.fields]
        extras = set(self.df.columns) - set(expected_names)
        if extras:
            self.invalid_df = self.invalid_df.withColumn("invalid_extra_columns", F.lit(list(extras)))

    def test_required_not_null_and_ranges_and_domain(self):
        """Nulls, non-negative numeric ranges, and enum set for page."""
        nulls = self.df.filter(
            F.col("user_id").isNull()
            | F.col("timestamp").isNull()
            | F.col("page").isNull()
            | F.col("duration_seconds").isNull()
        )
        if nulls.count() > 0:
            self.invalid_df = self.invalid_df.withColumn("invalid_nulls", F.lit(True))
        ranges = self.df.filter((F.col("user_id") < 0) | (F.col("duration_seconds") < 0))
        if ranges.count() > 0:
            self.invalid_df = self.invalid_df.withColumn("invalid_ranges", F.lit(True))
        domain = self.df.filter(~F.col("page").isin(*ALLOWED_PAGES))
        if domain.count() > 0:
            self.invalid_df = self.invalid_df.withColumn("invalid_domain", F.lit(True))

    def test_timestamp_parseable(self):
        """String timestamps must be parseable with the agreed pattern."""
        parsed = self.df.withColumn("ts", F.to_timestamp("timestamp", PATTERN))
        if parsed.filter(F.col("ts").isNull()).count() > 0:
            self.invalid_df = self.invalid_df.withColumn("invalid_timestamp", F.lit(True))

    def export_invalid_to_csv(self, output_dir: str):
        """Export invalid rows to CSV for manual review."""
        from pyspark.sql.functions import array, col
        import datetime
        manual_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        batch_id = manual_datetime.replace("-","").replace(":","").replace(" ","")
        invalid_columns = [c for c in self.invalid_df.columns if c.startswith("invalid_")]
        result_df = self.invalid_df.select(
            F.lit(batch_id).alias("manual_datetime"),
            "timestamp",
            *[col(c) for c in self.df.columns],
            array([col(c) for c in invalid_columns]).alias("invalid_columns")
        )
        result_df.coalesce(1).write.csv(os.path.join(output_dir, "manual_datetime"), header=True, mode="overwrite")

