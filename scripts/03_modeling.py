#!/usr/bin/env python3
"""Temperature-performance modeling using pyspark.ml."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


MODEL_SCHEMA = T.StructType(
    [
        T.StructField("model", T.StringType(), False),
        T.StructField("n", T.IntegerType(), False),
        T.StructField("intercept", T.DoubleType(), True),
        T.StructField("temp_coef", T.DoubleType(), True),
        T.StructField("temp_sq_coef", T.DoubleType(), True),
        T.StructField("r_squared", T.DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run baseline modeling on the cleaned marathon parquet dataset.")
    parser.add_argument(
        "--input",
        default="outputs/processed/marathon_joined",
        help="Input parquet dataset path. Can be local or HDFS.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/tables/modeling",
        help="Local output directory for modeling tables.",
    )
    parser.add_argument(
        "--app-name",
        default="BerlinMarathonModeling",
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

    # Spark writes CSV as a directory of part files. We keep one part per table
    # so the downstream figure script can locate a single stable CSV easily.
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(target.resolve().as_uri())
    )


def build_model_frame(df: DataFrame, feature_cols: list[str], label_col: str) -> DataFrame:
    model_df = df.select(*(feature_cols + [label_col])).dropna()
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    return assembler.transform(model_df).select("features", F.col(label_col).alias("label"))


def fit_linear_model(
    df: DataFrame,
    feature_cols: list[str],
    label_col: str,
    model_name: str,
) -> dict[str, float | str | int | None]:
    model_df = build_model_frame(df, feature_cols, label_col)
    row_count = model_df.count()

    # fitIntercept=True matches the conventional regression specification used
    # in the written report and keeps model outputs interpretable.
    lr = LinearRegression(featuresCol="features", labelCol="label", fitIntercept=True)
    fitted = lr.fit(model_df)
    coefficients = list(fitted.coefficients)

    temp_coef = coefficients[0] if len(coefficients) >= 1 else None
    temp_sq_coef = coefficients[1] if len(coefficients) >= 2 else None

    return {
        "model": model_name,
        "n": row_count,
        "intercept": float(fitted.intercept),
        "temp_coef": None if temp_coef is None else float(temp_coef),
        "temp_sq_coef": None if temp_sq_coef is None else float(temp_sq_coef),
        "r_squared": float(fitted.summary.r2),
    }


def rows_to_df(spark: SparkSession, rows: list[dict[str, float | str | int | None]], extra_fields: list[T.StructField] | None = None) -> DataFrame:
    schema_fields = list(MODEL_SCHEMA.fields)
    if extra_fields:
        schema_fields.extend(extra_fields)
    schema = T.StructType(schema_fields)
    return spark.createDataFrame(rows, schema=schema)


def grouped_linear_results(
    spark: SparkSession,
    df: DataFrame,
    group_col: str,
    label_col: str,
) -> DataFrame:
    group_values = [row[group_col] for row in df.select(group_col).distinct().collect()]
    rows: list[dict[str, float | str | int | None]] = []

    # Fit one comparable linear model per subgroup so the temperature slope can
    # be compared directly across gender and performance strata.
    for group_value in group_values:
        result = fit_linear_model(
            df.filter(F.col(group_col) == group_value),
            ["AVG_TEMP_C"],
            label_col,
            f"{group_col}={group_value}",
        )
        result[group_col] = None if group_value is None else str(group_value)
        rows.append(result)

    return rows_to_df(spark, rows, [T.StructField(group_col, T.StringType(), True)])


def build_modeling_tables(spark: SparkSession, df: DataFrame) -> dict[str, DataFrame]:
    # Keep the primary outputs explicit: one linear model for readability and
    # one quadratic model to test the proposed non-linear temperature effect.
    overall_models = rows_to_df(
        spark,
        [
            fit_linear_model(df, ["AVG_TEMP_C"], "finish_time_minutes", "overall_linear"),
            fit_linear_model(df, ["AVG_TEMP_C", "avg_temp_squared"], "finish_time_minutes", "overall_quadratic"),
        ],
    )

    yearly_temp_effect = (
        df.groupBy("YEAR")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
            F.expr("percentile_approx(finish_time_minutes, 0.5)").alias("median_finish_time_minutes"),
            F.first("AVG_TEMP_C").alias("avg_temp_c"),
            F.first("MAX_TEMP_C").alias("max_temp_c"),
            F.first("MIN_TEMP_C").alias("min_temp_c"),
        )
        .orderBy("YEAR")
    )

    gender_models = grouped_linear_results(spark, df, "GENDER", "finish_time_minutes").orderBy("GENDER")
    performance_models = grouped_linear_results(spark, df, "performance_group", "finish_time_minutes").orderBy("performance_group")

    temperature_grouped_means = (
        df.groupBy("temperature_bin", "GENDER", "performance_group")
        .agg(
            F.count("*").alias("runner_count"),
            F.avg("finish_time_minutes").alias("avg_finish_time_minutes"),
        )
        .orderBy("temperature_bin", "GENDER", "performance_group")
    )

    yearly_temperature_extremes = (
        yearly_temp_effect
        # Dense rank is enough here because we only want the hottest/coldest few
        # years for a simple report figure, not a fully unique ordering.
        .withColumn("heat_rank", F.dense_rank().over(Window.orderBy(F.desc("avg_temp_c"))))
        .withColumn("cold_rank", F.dense_rank().over(Window.orderBy(F.asc("avg_temp_c"))))
        .filter((F.col("heat_rank") <= 5) | (F.col("cold_rank") <= 5))
        .orderBy("avg_temp_c")
    )

    return {
        "overall_models": overall_models,
        "yearly_temp_effect": yearly_temp_effect,
        "gender_models": gender_models,
        "performance_models": performance_models,
        "temperature_grouped_means": temperature_grouped_means,
        "yearly_temperature_extremes": yearly_temperature_extremes,
    }


def write_summary_text(output_dir: Path, overall_models: DataFrame) -> None:
    rows = {row["model"]: row.asDict() for row in overall_models.collect()}
    linear = rows["overall_linear"]
    quadratic = rows["overall_quadratic"]
    # This text file is a quick checkpoint for report writing and debugging
    # without having to open the CSV folders generated by Spark.
    content = [
        "Berlin Marathon Modeling Summary",
        f"overall_linear_temp_coef={linear['temp_coef']:.4f}",
        f"overall_linear_r_squared={linear['r_squared']:.6f}",
        f"overall_quadratic_temp_coef={quadratic['temp_coef']:.4f}",
        f"overall_quadratic_temp_sq_coef={quadratic['temp_sq_coef']:.4f}",
        f"overall_quadratic_r_squared={quadratic['r_squared']:.6f}",
    ]
    (output_dir / "modeling_summary.txt").write_text("\n".join(content) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    spark = build_spark(args.app_name)
    output_dir = prepare_output_dir(args.output_dir)

    df = spark.read.parquet(args.input)
    tables = build_modeling_tables(spark, df)

    write_summary_text(output_dir, tables["overall_models"])
    for name, table in tables.items():
        write_single_csv(table, output_dir, name)
        print(f"[done] wrote {output_dir / name}")

    spark.stop()


if __name__ == "__main__":
    main()
