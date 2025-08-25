from pyspark.sql import functions as F

PATTERN = "yyyy-MM-dd HH:mm:ss"
EXPECTED_COLS = ["user_id", "timestamp", "page", "duration_seconds"]
EXPECTED_TYPES = {
    "user_id": "int",
    "timestamp": "string",    # stored as string at ingress; you parse before use
    "page": "string",
    "duration_seconds": "int",
}
ALLOWED_PAGES = {"home", "dashboard", "profile"}

def _valid_df(spark):
    data = [
        (1, "2022-01-01 12:00:00", "home",      30),
        (2, "2022-01-01 12:05:00", "dashboard", 45),
        (3, "2022-01-01 12:10:00", "profile",   60),
    ]
    schema = "user_id INT, timestamp STRING, page STRING, duration_seconds INT"
    return spark.createDataFrame(data, schema=schema)

def test_schema_columns_and_types(spark):
    df = _valid_df(spark)
    # exact columns
    assert df.columns == EXPECTED_COLS
    # types
    got_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert got_types == EXPECTED_TYPES

def test_no_extra_columns_detected(spark):
    df = _valid_df(spark)
    # If someone upstream adds a column and you select("*"), this catches it
    df_extra = df.withColumn("referrer", F.lit("google"))
    extras = set(df_extra.columns) - set(EXPECTED_COLS)
    assert extras == {"referrer"}  # detection test; your pipeline would fail/route to quarantine

def test_values_in_domain_and_ranges(spark):
    df = _valid_df(spark)
    # required cols not null
    nulls = df.filter(
        F.col("user_id").isNull() |
        F.col("timestamp").isNull() |
        F.col("page").isNull() |
        F.col("duration_seconds").isNull()
    )
    assert nulls.count() == 0

    # ranges
    viol = df.filter((F.col("user_id") < 0) | (F.col("duration_seconds") < 0))
    assert viol.count() == 0

    # enum for page
    bad_page = df.filter(~F.col("page").isin(*ALLOWED_PAGES))
    assert bad_page.count() == 0

def test_timestamp_parseable(spark):
    df = _valid_df(spark)
    parsed = df.withColumn("ts", F.to_timestamp("timestamp", PATTERN))
    bad = parsed.filter(F.col("ts").isNull())
    assert bad.count() == 0
