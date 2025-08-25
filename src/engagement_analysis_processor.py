from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class EngagementAnalysisProcessor:
    """Processor for analyzing user engagement data."""

    def __init__(self, df: DataFrame):
        """
        Initialize the processor.

        Args:
            df (DataFrame): PySpark DataFrame with columns:
        """
        self.df = df

    def avg_duration_per_page(self, df: DataFrame = None) -> DataFrame:
        """
        Calculate average duration spent on each page.

        Args:
            df (DataFrame, optional): If provided, overrides default df.

        Returns:
            DataFrame: page, avg_duration_sec, sorted by page.
        """
        df = df or self.df
        return (
            df.groupBy("page")
            .agg(F.avg("duration_seconds").alias("avg_duration_sec"))
            .orderBy("page")
        )

    def most_engaging_page(self, df: DataFrame = None) -> DataFrame:
        """
        Find the page with the highest average duration.

        Args:
            df (DataFrame, optional): If provided, overrides default df.

        Returns:
            DataFrame: single row with most engaging page.
        """
        avg_df = self.avg_duration_per_page(df)
        return avg_df.orderBy(F.col("avg_duration_sec").desc()).limit(1)
