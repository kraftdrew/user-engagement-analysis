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

    input_path = "dbfs:/FileStore/gore_csv/user_engagement.csv"
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("page", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
    ])
    
    processor = EngagementAnalysisProcessor()

    df = processor.build_df_from_dbfs(spark, input_path, schema)

    avg_df = processor.avg_duration_per_page(df)
    top_row = processor.most_engaging_page(df).collect()[0]

    print("Average Duration Per Page:\n")
    avg_df.show(truncate=False)
    print("")

    print(f"Most engaging page: {top_row['page']} (average duration: {top_row['avg_duration_sec']} seconds)")


if __name__ == "__main__":
    main()


