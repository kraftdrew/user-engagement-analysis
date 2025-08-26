import datetime
import os
from pyspark.sql.functions import col, expr
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

PATTERN = "yyyy-MM-dd HH:mm:ss"

EXPECTED_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),  # ingest as string
        StructField("page", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
    ]
)

ALLOWED_PAGES = {"home", "dashboard", "profile"}


class UserEngagementDQ:
    def __init__(self, df: DataFrame) -> None:
        self.df = df
        self.df_with_dq_flags = df

    def test_required_not_null_and_ranges_and_domain(self) -> None:
        """Nulls, non-negative numeric ranges, and enum set for page."""
        self.df_with_dq_flags = self.df_with_dq_flags.withColumn(
            "invalid_nulls",
            F.when(
                F.col("user_id").isNull()
                | F.col("timestamp").isNull()
                | F.col("page").isNull()
                | F.col("duration_seconds").isNull(),
                True,
            ).otherwise(False),
        )
        self.df_with_dq_flags = self.df_with_dq_flags.withColumn(
            "invalid_id_and_duration_ranges",
            F.when(
                (F.col("user_id") < 0) | (F.col("duration_seconds") < 0),
                True,
            ).otherwise(False),
        )
        self.df_with_dq_flags = self.df_with_dq_flags.withColumn(
            "invalid_domain",
            F.when(~F.col("page").isin(*ALLOWED_PAGES), True).otherwise(False),
        )

    def test_timestamp_parseable(self) -> None:
        """String timestamps must be parseable with the agreed pattern."""
        self.df_with_dq_flags = self.df_with_dq_flags.withColumn(
            "invalid_timestamp",
            F.when(F.to_timestamp("timestamp", PATTERN).isNull(), True).otherwise(False),
        )

    def log_invalid_records(self, output_dir: str = 'dbfs:/FileStore/gore_logs') -> DataFrame:
        """Log invalid records to parquet files with timestamp."""
        batch_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        ingest_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        invalid_columns = [c for c in self.df_with_dq_flags.columns if c.startswith("invalid_")]
        if not invalid_columns:
            return self.df_with_dq_flags.limit(0)  # No checks run, return empty
        condition = " OR ".join([f"{c} = True" for c in invalid_columns])
        result_df = self.df_with_dq_flags.filter(expr(condition))

        result_df = result_df.select(
            *[col(c) for c in self.df.columns],
            F.lit(ingest_datetime).alias("__ingest_datetime")
        )
        result_df.write.parquet(os.path.join(output_dir, batch_id), mode="overwrite")
        print(f"Exported invalid rows to: {os.path.join(output_dir, batch_id)}")
        return result_df

    def get_df_with_valid_rows(self) -> DataFrame:
        """Run all checks, write invalid records to Parquet, and return only valid records as DataFrame."""
        invalid_columns = [c for c in self.df_with_dq_flags.columns if c.startswith("invalid_")]
        valid_condition = " AND ".join([f"({c} = False OR {c} IS NULL)" for c in invalid_columns])
        valid_df = self.df_with_dq_flags.filter(expr(valid_condition)).select(*self.df.columns)
        return valid_df
