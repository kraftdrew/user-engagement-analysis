import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.engagement_analysis_processor import EngagementAnalysisProcessor


# Use the static loader on EngagementAnalysisProcessor instead of a local helper


def main() -> None:
    # Use the active session in Databricks (no need to create/stop SparkSession)
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found. Run inside Databricks or create a session.")

    input_path = "dbfs:/FileStore/gore_csv/user_engagement_invalid.csv"
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("page", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
    ])
    
    processor = EngagementAnalysisProcessor()

    df = processor.build_df_from_dbfs(spark, input_path, schema)

    top_row = processor.most_engaging_page(df).collect()[0]
    print(f"Most engaging page: {top_row['page']} ({top_row['avg_duration_sec']} sec)")

    for row in processor.avg_duration_per_page(df).collect():
        print(f"Avg on {row['page']}: {row['avg_duration_sec']} sec")


if __name__ == "__main__":
    main()


