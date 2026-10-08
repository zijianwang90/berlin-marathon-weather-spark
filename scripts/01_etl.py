#!/usr/bin/env python3
"""Spark ETL pipeline for the Berlin Marathon weather project."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("YEAR", T.IntegerType(), True),
        T.StructField("COUNTRY", T.StringType(), True),
        T.StructField("GENDER", T.StringType(), True),
        T.StructField("AGE", T.StringType(), True),
        T.StructField("TIME", T.StringType(), True),
    ]
)

WEATHER_SCHEMA = T.StructType(
    [
        T.StructField("YEAR", T.IntegerType(), True),
        T.StructField("PRECIP_mm", T.DoubleType(), True),
        T.StructField("SUNSHINE_hrs", T.DoubleType(), True),
        T.StructField("CLOUD_hrs", T.DoubleType(), True),
        T.StructField("ATMOS_PRESS_mbar", T.DoubleType(), True),
        T.StructField("AVG_TEMP_C", T.DoubleType(), True),
        T.StructField("MAX_TEMP_C", T.DoubleType(), True),
        T.StructField("MIN_TEMP_C", T.DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and join Berlin Marathon data with Spark.")
    parser.add_argument(
        "--results",
        default="src/Berlin_Marathon_data_1974_2019.csv",
        help="Path to runner-level marathon results CSV.",
    )
    parser.add_argument(
        "--weather",
        default="src/Berlin_Marathon_weather_data_since_1974.csv",
        help="Path to weather summary CSV.",
    )
    parser.add_argument(
        "--output",
        default="outputs/processed/marathon_joined",
        help="Output directory for parquet dataset.",
    )
    parser.add_argument(
        "--app-name",
        default="BerlinMarathonETL",
        help="Spark application name.",
    )
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_results(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(RESULTS_SCHEMA)
        .csv(path)
        .withColumn("COUNTRY", F.trim(F.col("COUNTRY")))
        .withColumn("GENDER", F.lower(F.trim(F.col("GENDER"))))
        .withColumn("AGE_RAW", F.trim(F.col("AGE")))
        .withColumn("TIME_RAW", F.trim(F.col("TIME")))
    )


def read_weather(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(WEATHER_SCHEMA).csv(path)


def add_time_features(df: DataFrame) -> DataFrame:
    parts = F.split(F.col("TIME_RAW"), ":")
    hours = parts.getItem(0).cast("int")
    minutes = parts.getItem(1).cast("int")
    seconds = parts.getItem(2).cast("int")

    valid_time = F.col("TIME_RAW").rlike(r"^\d{2}:\d{2}:\d{2}$")
    total_seconds = (hours * 3600) + (minutes * 60) + seconds

    return (
        df.withColumn("time_is_valid", valid_time)
        .withColumn(
            "finish_time_seconds",
            F.when(valid_time, total_seconds).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "finish_time_minutes",
            F.when(valid_time, F.round(F.col("finish_time_seconds") / 60.0, 2)).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn("finish_hours", F.when(valid_time, hours))
    )


def normalize_age(df: DataFrame) -> DataFrame:
    numeric_age = F.when(F.col("AGE_RAW").rlike(r"^\d+$"), F.col("AGE_RAW").cast("int"))

    return (
        df.withColumn("age_numeric", numeric_age)
        .withColumn(
            "age_group_standardized",
            F.when(F.col("age_numeric").isNotNull(), F.concat(F.lit("age_"), F.col("AGE_RAW")))
            .when(F.col("AGE_RAW").isin("L", "L1", "L2", "L3", "L4"), F.lit("legacy_L_group"))
            .when(F.col("AGE_RAW").startswith("D"), F.lit("legacy_D_group"))
            .when(F.col("AGE_RAW").isin("A", "B", "BM", "M", "M0", "M<", "Ber", "C"), F.lit("other_code"))
            .otherwise(F.lit("unknown_code")),
        )
    )


def filter_implausible_finish_times(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("finish_time_seconds").isNotNull()
        & (F.col("finish_time_seconds") >= 2 * 3600)
        & (F.col("finish_time_seconds") <= 10 * 3600)
    )


def add_analysis_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "temperature_bin",
            F.when(F.col("AVG_TEMP_C") < 8, F.lit("lt_8C"))
            .when(F.col("AVG_TEMP_C") < 12, F.lit("8_to_12C"))
            .when(F.col("AVG_TEMP_C") < 16, F.lit("12_to_16C"))
            .otherwise(F.lit("ge_16C")),
        )
        .withColumn(
            "performance_group",
            F.when(F.col("finish_time_seconds") < 3 * 3600, F.lit("fast"))
            .when(F.col("finish_time_seconds") < 4 * 3600, F.lit("mid"))
            .otherwise(F.lit("slow")),
        )
        .withColumn("avg_temp_squared", F.pow(F.col("AVG_TEMP_C"), 2))
    )


def join_results_weather(results_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    return results_df.join(weather_df, on="YEAR", how="left")


def print_quality_checks(raw_results: DataFrame, cleaned_results: DataFrame, joined_df: DataFrame) -> None:
    print(f"[quality] raw_results_rows={raw_results.count()}")
    print(f"[quality] cleaned_results_rows={cleaned_results.count()}")
    print(f"[quality] joined_rows={joined_df.count()}")
    print(
        "[quality] missing_weather_rows="
        f"{joined_df.filter(F.col('AVG_TEMP_C').isNull()).count()}"
    )
    joined_df.groupBy("GENDER").count().orderBy("count", ascending=False).show(truncate=False)
    joined_df.groupBy("temperature_bin").count().orderBy("temperature_bin").show(truncate=False)


def main() -> None:
    args = parse_args()
    spark = build_spark(args.app_name)

    raw_results = read_results(spark, args.results)
    weather_df = read_weather(spark, args.weather)

    cleaned_results = add_time_features(raw_results)
    cleaned_results = normalize_age(cleaned_results)
    cleaned_results = filter_implausible_finish_times(cleaned_results)

    joined_df = join_results_weather(cleaned_results, weather_df)
    final_df = add_analysis_features(joined_df)

    print_quality_checks(raw_results, cleaned_results, final_df)

    (
        final_df.repartition(1)
        .write.mode("overwrite")
        .parquet(args.output)
    )

    print(f"[done] parquet written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
