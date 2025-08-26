from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType


class EngagementAnalysisProcessor:
    def __init__(self) -> None:
        """Initialize the EngagementAnalysisProcessor."""
        pass

    def build_df_from_dbfs(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
        """
        Read user engagement CSV from DBFS with a fixed schema.

        Args:
            spark (SparkSession): Active Spark session (Databricks-friendly).
            path (str): DBFS path to the CSV file.
            schema (StructType): Schema to apply when reading the CSV.

        Returns:
            DataFrame: Loaded DataFrame with expected columns and types.
        """
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .schema(schema)
            .load(path)
        )

    def avg_duration_per_page(self, df: DataFrame) -> DataFrame:
        """
        Calculate average duration spent on each page.

        Args:
            df (DataFrame): Input DataFrame with user engagement data.

        Returns:
            DataFrame: page, avg_duration_sec, sorted by page.
        """
        return (
            df.groupBy("page")
            .agg(F.round(F.avg("duration_seconds"), 1).alias("avg_duration_sec"))
            .orderBy("page")
        )

    def most_engaging_page(self, df: DataFrame) -> DataFrame:
        """
        Find the page with the highest average duration.

        Args:
            df (DataFrame): Input DataFrame with user engagement data.

        Returns:
            DataFrame: single row with most engaging page.
        """
        avg_df = self.avg_duration_per_page(df)
        return avg_df.orderBy(F.col("avg_duration_sec").desc()).limit(1)

