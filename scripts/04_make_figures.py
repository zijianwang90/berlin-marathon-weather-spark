#!/usr/bin/env python3
"""Generate report figures using matplotlib/seaborn."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate matplotlib figures from project tables.")
    parser.add_argument("--eda-dir", default="outputs/tables/eda", help="Directory containing EDA CSV folders.")
    parser.add_argument(
        "--model-dir",
        default="outputs/tables/modeling",
        help="Directory containing modeling CSV folders.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/figures",
        help="Directory for figure outputs.",
    )
    return parser.parse_args()


def find_csv(directory: Path) -> Path:
    # Spark writes each logical table as a folder containing one part CSV.
    candidates = sorted(path for path in directory.glob("part-*.csv") if path.is_file())
    if not candidates:
        raise FileNotFoundError(f"No CSV file found under {directory}")
    return candidates[0]


def load_table(root: Path, name: str) -> pd.DataFrame:
    return pd.read_csv(find_csv(root / name))


def setup_style() -> None:
    # Use one shared style so all report figures look like the same project
    # instead of a mix of ad hoc plotting defaults.
    sns.set_theme(style="whitegrid")
    plt.rcParams.update(
        {
            "figure.facecolor": "#f8f4ea",
            "axes.facecolor": "#f8f4ea",
            "axes.edgecolor": "#222222",
            "axes.labelcolor": "#222222",
            "text.color": "#222222",
            "xtick.color": "#222222",
            "ytick.color": "#222222",
            "grid.color": "#d7cfbf",
            "font.family": "DejaVu Serif",
            "axes.titleweight": "bold",
        }
    )


def save_current(fig: plt.Figure, output_path: Path) -> None:
    fig.tight_layout()
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    eda_dir = Path(args.eda_dir)
    model_dir = Path(args.model_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_style()

    yearly = load_table(eda_dir, "yearly_summary")
    temperature_bin = load_table(eda_dir, "temperature_bin_summary")
    gender = load_table(eda_dir, "gender_summary")
    overall = load_table(model_dir, "overall_models")
    gender_models = load_table(model_dir, "gender_models")
    performance_models = load_table(model_dir, "performance_models")
    yearly_extremes = load_table(model_dir, "yearly_temperature_extremes")

    # Figure 1: macro participation trend over time.
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    sns.lineplot(data=yearly, x="YEAR", y="runner_count", marker="o", color="#b44f2a", linewidth=2.5, ax=ax)
    ax.set_title("Berlin Marathon Participation by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Runner Count")
    save_current(fig, output_dir / "figure_01_participation_by_year.png")

    # Figure 2: long-run finish-time trend.
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    sns.lineplot(data=yearly, x="YEAR", y="avg_finish_time_minutes", marker="o", color="#2f6c62", linewidth=2.5, ax=ax)
    ax.set_title("Average Finish Time by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Average Finish Time (minutes)")
    save_current(fig, output_dir / "figure_02_avg_finish_time_by_year.png")

    # Figure 3: yearly average weather-performance association.
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    sns.regplot(
        data=yearly,
        x="avg_temp_c",
        y="avg_finish_time_minutes",
        scatter_kws={"s": 70, "color": "#2f6c62", "alpha": 0.8},
        line_kws={"color": "#b44f2a", "linewidth": 2.5},
        ax=ax,
    )
    ax.set_title("Average Temperature vs Average Finish Time")
    ax.set_xlabel("Average Race-Day Temperature (C)")
    ax.set_ylabel("Average Finish Time (minutes)")
    save_current(fig, output_dir / "figure_03_temp_vs_finish_scatter.png")

    # Figure 4: easier-to-read grouped comparison for the report narrative.
    fig, ax = plt.subplots(figsize=(9.2, 5.8))
    order = ["lt_8C", "8_to_12C", "12_to_16C", "ge_16C"]
    sns.barplot(
        data=temperature_bin,
        x="temperature_bin",
        y="avg_finish_time_minutes",
        order=order,
        palette=["#6a4c93", "#2f6c62", "#d29b2d", "#b44f2a"],
        ax=ax,
    )
    ax.set_title("Average Finish Time by Temperature Bin")
    ax.set_xlabel("Temperature Bin")
    ax.set_ylabel("Average Finish Time (minutes)")
    save_current(fig, output_dir / "figure_04_temp_bin_finish_time.png")

    # Figure 5: simple gender comparison from the EDA tables.
    fig, ax = plt.subplots(figsize=(7.4, 5.8))
    sns.barplot(
        data=gender,
        x="GENDER",
        y="avg_finish_time_minutes",
        palette=["#d29b2d", "#b44f2a"],
        ax=ax,
    )
    ax.set_title("Average Finish Time by Gender")
    ax.set_xlabel("Gender")
    ax.set_ylabel("Average Finish Time (minutes)")
    save_current(fig, output_dir / "figure_05_gender_finish_time.png")

    # Combine subgroup slopes into one compact panel instead of scattering them
    # across separate tables in the final report.
    coef_df = pd.DataFrame(
        {
            "group": ["female", "male", "fast", "mid", "slow"],
            "temp_coef": [
                gender_models.loc[gender_models["GENDER"] == "female", "temp_coef"].iloc[0],
                gender_models.loc[gender_models["GENDER"] == "male", "temp_coef"].iloc[0],
                performance_models.loc[performance_models["performance_group"] == "fast", "temp_coef"].iloc[0],
                performance_models.loc[performance_models["performance_group"] == "mid", "temp_coef"].iloc[0],
                performance_models.loc[performance_models["performance_group"] == "slow", "temp_coef"].iloc[0],
            ],
        }
    )
    # Figure 6: estimated marginal temperature effect by subgroup.
    fig, ax = plt.subplots(figsize=(9.0, 5.8))
    sns.barplot(data=coef_df, x="group", y="temp_coef", color="#b44f2a", ax=ax)
    ax.set_title("Estimated Temperature Sensitivity by Runner Group")
    ax.set_xlabel("Runner Group")
    ax.set_ylabel("Estimated Minutes Added per 1C")
    save_current(fig, output_dir / "figure_06_temp_sensitivity_by_group.png")

    # Figure 7: highlight the most extreme race-weather years.
    fig, ax = plt.subplots(figsize=(9.2, 5.8))
    top_extremes = yearly_extremes.sort_values("avg_temp_c")
    sns.barplot(data=top_extremes, x="YEAR", y="avg_temp_c", color="#2f6c62", ax=ax)
    ax.set_title("Hottest and Coldest Marathon Years")
    ax.set_xlabel("Year")
    ax.set_ylabel("Average Temperature (C)")
    ax.tick_params(axis="x", rotation=45)
    save_current(fig, output_dir / "figure_07_yearly_temperature_extremes.png")

    quadratic = overall.loc[overall["model"] == "overall_quadratic"].iloc[0]
    summary = [
        "Matplotlib Figures",
        "figure_01_participation_by_year.png",
        "figure_02_avg_finish_time_by_year.png",
        "figure_03_temp_vs_finish_scatter.png",
        "figure_04_temp_bin_finish_time.png",
        "figure_05_gender_finish_time.png",
        "figure_06_temp_sensitivity_by_group.png",
        "figure_07_yearly_temperature_extremes.png",
        f"quadratic_temp_coef={quadratic['temp_coef']:.4f}",
        f"quadratic_temp_sq_coef={quadratic['temp_sq_coef']:.4f}",
    ]
    (output_dir / "figures_manifest.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
