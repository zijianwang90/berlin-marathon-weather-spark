#!/usr/bin/env python3
"""Descriptive analysis for the Berlin Marathon weather dataset."""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run EDA on the cleaned Berlin Marathon parquet dataset.")
    parser.add_argument(
        "--input",
        default="outputs/processed/marathon_joined",
        help="Input parquet dataset path. Can be local or HDFS.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/tables/eda",
        help="Local output directory for CSV summary tables.",
    )
    parser.add_argument(
        "--app-name",
        default="BerlinMarathonEDA",
        help="Spark application name.",
    )
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def prepare_output_dir(path: str) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_single_csv(df: DataFrame, output_dir: Path, name: str) -> None:
    target = output_dir / name
    if target.exists():
        shutil.rmtree(target)

    target_uri = target.resolve().as_uri()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(target_uri)
    )


def write_text_summary(df: DataFrame, output_dir: Path) -> None:
    metrics = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("YEAR").alias("years"),
        F.min("YEAR").alias("first_year"),
        F.max("YEAR").alias("last_year"),
        F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
        F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
        F.min("AVG_TEMP_C").alias("min_avg_temp_c"),
        F.max("AVG_TEMP_C").alias("max_avg_temp_c"),
    ).collect()[0]

    content = [
        "Berlin Marathon EDA Summary",
        f"rows={metrics['rows']}",
        f"years={metrics['years']}",
        f"year_range={metrics['first_year']}-{metrics['last_year']}",
        f"avg_finish_time_minutes={metrics['avg_finish_time_minutes']:.2f}",
        f"median_finish_time_minutes={metrics['median_finish_time_minutes']:.2f}",
        f"avg_temp_range_c={metrics['min_avg_temp_c']:.1f} to {metrics['max_avg_temp_c']:.1f}",
    ]

    (output_dir / "eda_summary.txt").write_text("\n".join(content) + "\n", encoding="utf-8")


def build_tables(df: DataFrame) -> dict[str, DataFrame]:
    yearly_summary = (
        df.groupBy("YEAR")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
            F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
            F.first("AVG_TEMP_C").alias("avg_temp_c"),
            F.first("MAX_TEMP_C").alias("max_temp_c"),
            F.first("MIN_TEMP_C").alias("min_temp_c"),
            F.first("PRECIP_mm").alias("precip_mm"),
            F.first("SUNSHINE_hrs").alias("sunshine_hrs"),
        )
        .orderBy("YEAR")
    )

    gender_summary = (
        df.groupBy("GENDER")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
            F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
        )
        .orderBy("GENDER")
    )

    performance_summary = (
        df.groupBy("performance_group")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
            F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
        )
        .orderBy("performance_group")
    )

    temperature_bin_summary = (
        df.groupBy("temperature_bin")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
            F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
            F.avg("AVG_TEMP_C").alias("mean_bin_temp_c"),
        )
        .orderBy("temperature_bin")
    )

    gender_temperature_summary = (
        df.groupBy("GENDER", "temperature_bin")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
        )
        .orderBy("GENDER", "temperature_bin")
    )

    performance_temperature_summary = (
        df.groupBy("performance_group", "temperature_bin")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
        )
        .orderBy("performance_group", "temperature_bin")
    )

    age_group_summary = (
        df.groupBy("age_group_standardized")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
        )
        .orderBy(F.desc("runner_count"))
    )

    weather_correlation_ready = (
        df.select(
            "YEAR",
            "finish_time_minutes",
            "AVG_TEMP_C",
            "MAX_TEMP_C",
            "MIN_TEMP_C",
            "PRECIP_mm",
            "SUNSHINE_hrs",
            "CLOUD_hrs",
            "ATMOS_PRESS_mbar",
            "GENDER",
            "performance_group",
            "temperature_bin",
        )
        .orderBy("YEAR")
    )

    return {
        "yearly_summary": yearly_summary,
        "gender_summary": gender_summary,
        "performance_summary": performance_summary,
        "temperature_bin_summary": temperature_bin_summary,
        "gender_temperature_summary": gender_temperature_summary,
        "performance_temperature_summary": performance_temperature_summary,
        "age_group_summary": age_group_summary,
        "weather_correlation_ready": weather_correlation_ready,
    }


def main() -> None:
    args = parse_args()
    spark = build_spark(args.app_name)
    output_dir = prepare_output_dir(args.output_dir)

    df = spark.read.parquet(args.input)
    tables = build_tables(df)

    write_text_summary(df, output_dir)
    for name, table in tables.items():
        write_single_csv(table, output_dir, name)
        print(f"[done] wrote {output_dir / name}")

    spark.stop()


if __name__ == "__main__":
    main()
