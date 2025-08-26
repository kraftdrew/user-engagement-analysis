from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class EngagementAnalysisProcessor:

    def avg_duration_per_page(self, df: DataFrame) -> DataFrame:
        """
        Calculate average duration spent on each page.

        Args:
            df (DataFrame, optional): If provided, overrides default df.

        Returns:
            DataFrame: page, avg_duration_sec, sorted by page.
        """
        return (
            df.groupBy("page")
            .agg(F.round(F.avg("duration_seconds"),1).alias("avg_duration_sec"))
            .orderBy("page")
        )

    def most_engaging_page(self, df: DataFrame) -> DataFrame:
        """
        Find the page with the highest average duration.

        Args:
            df (DataFrame, optional): If provided, overrides default df.

        Returns:
            DataFrame: single row with most engaging page.
        """
        avg_df = self.avg_duration_per_page(df)
        return avg_df.orderBy(F.col("avg_duration_sec").desc()).limit(1)

