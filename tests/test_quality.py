from pyspark.sql import SparkSession
from jobs.spark.lib.quality import run_quality_suite

def test_quality_suite_runs():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [("e1", "view", 1), ("e2", "click", 2)],
        ["event_id", "event_type", "user_id"],
    )
    results = run_quality_suite(df, curated_rows=2)
    assert any(r[0] == "drop_rate_guardrail" for r in results)
    spark.stop()
