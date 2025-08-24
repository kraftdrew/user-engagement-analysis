import pyspark.sql.functions as F

def avg_duration_per_page(df):
    """Return avg duration per page (page, avg_duration_sec), ordered by page."""
    return (
        df.groupBy("page")
          .agg(F.sum("duration_seconds").alias("avg_duration_sec"))
          .orderBy("page")
    )

def most_engaging_page(df):
    """Return a single-row DF with the page that has the highest avg duration."""
    avg_df = avg_duration_per_page(df)
    return avg_df.orderBy(F.col("avg_duration_sec").desc()).limit(1)
