from __future__ import annotations

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

from jobs.spark.lib.utils import get_paths
from jobs.spark.lib.quality import run_quality_suite

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("ts", T.StringType(), True),
    T.StructField("dt", T.StringType(), True),
    T.StructField("user_id", T.LongType(), True),
    T.StructField("session_id", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("subreddit", T.StringType(), True),
    T.StructField("device", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("ingested_at", T.StringType(), True),
])

def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def _dir_size_bytes(spark: SparkSession, path: str) -> int:
    """Compute directory size in bytes using Hadoop FS APIs."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return 0
    it = fs.listFiles(p, True)
    total = 0
    while it.hasNext():
        f = it.next()
        if f.isFile():
            total += f.getLen()
    return int(total)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_root", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--raw_file", required=True)
    parser.add_argument("--dt", required=True)
    parser.add_argument("--pg_url", required=True)
    parser.add_argument("--pg_user", required=True)
    parser.add_argument("--pg_password", required=True)
    args = parser.parse_args()

    spark = build_spark(f"dp-transform-{args.run_id}")
    paths = get_paths(args.project_root)

    raw_path = os.path.join(paths.raw, args.raw_file)
    curated_out = os.path.join(paths.curated, f"dt={args.dt}")
    metrics_out = os.path.join(paths.metrics, f"dt={args.dt}")

    df = (
        spark.read.schema(EVENT_SCHEMA).json(raw_path)
        .withColumn("event_ts", F.to_timestamp(F.col("ts")))
        .withColumn("ingested_ts", F.to_timestamp(F.col("ingested_at")))
        .withColumn("dt", F.to_date("dt"))
    )

    input_rows = df.count()

    df_clean = df.filter(F.col("event_id").isNotNull())
    df_dedup = df_clean.dropDuplicates(["event_id"])
    curated_rows = df_dedup.count()

    df_dedup.repartition(4).write.mode("overwrite").parquet(curated_out)

    daily = df_dedup.groupBy("dt", "event_type").agg(F.count("*").alias("cnt"))
    metrics_rows = daily.count()
    daily.write.mode("overwrite").parquet(metrics_out)

    curated_bytes = _dir_size_bytes(spark, curated_out)
    metrics_bytes = _dir_size_bytes(spark, metrics_out)

    props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}

    daily.write.jdbc(url=args.pg_url, table="daily_metrics", mode="overwrite", properties=props)

    results = run_quality_suite(df, curated_rows=curated_rows)
    q_rows = [(args.run_id, name, passed, details, datetime.utcnow()) for (name, passed, details) in results]
    q_schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("passed", T.BooleanType(), False),
        T.StructField("details", T.StringType(), True),
        T.StructField("created_at", T.TimestampType(), False),
    ])
    spark.createDataFrame(q_rows, q_schema).write.jdbc(
        url=args.pg_url, table="quality_results", mode="append", properties=props
    )

    stats_rows = [(
        args.run_id,
        datetime.strptime(args.dt, "%Y-%m-%d").date(),
        args.raw_file,
        int(input_rows),
        int(curated_rows),
        int(metrics_rows),
        int(curated_bytes),
        int(metrics_bytes),
        datetime.utcnow()
    )]
    stats_schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("dt", T.DateType(), False),
        T.StructField("raw_file", T.StringType(), False),
        T.StructField("input_rows", T.LongType(), False),
        T.StructField("curated_rows", T.LongType(), False),
        T.StructField("metrics_rows", T.LongType(), False),
        T.StructField("curated_bytes", T.LongType(), False),
        T.StructField("metrics_bytes", T.LongType(), False),
        T.StructField("created_at", T.TimestampType(), False),
    ])
    spark.createDataFrame(stats_rows, stats_schema).write.jdbc(
        url=args.pg_url, table="run_stats", mode="append", properties=props
    )

    manifest_rows = [(
        args.raw_file,
        datetime.strptime(args.dt, "%Y-%m-%d").date(),
        args.run_id,
        datetime.utcnow()
    )]
    manifest_schema = T.StructType([
        T.StructField("raw_file", T.StringType(), False),
        T.StructField("dt", T.DateType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("processed_at", T.TimestampType(), False),
    ])
    try:
        spark.createDataFrame(manifest_rows, manifest_schema).write.jdbc(
            url=args.pg_url, table="processed_files", mode="append", properties=props
        )
    except Exception:
        pass

    spark.stop()

if __name__ == "__main__":
    main()
