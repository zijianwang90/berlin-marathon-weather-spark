# Weather Effects on Berlin Marathon Performance

A distributed big-data analysis of how race-day weather affects marathon finishing times, built with **Apache Spark (PySpark, Spark MLlib)** and **Hadoop HDFS**.

> Course project for AMOD 5410H Big Data, M.Sc. in Applied Modelling & Quantitative Methods, Trent University (Winter 2026).
> Team: Zijian Wang, Xiaohan Zhang, Yuanyuan Zhang. Full paper: [`report/report.pdf`](report/report.pdf)

## Key Findings

- **Data:** 882,539 runner-level finishes across 44 race years (1974–2019), joined with race-day weather.
- **Warmer is slower:** in the linear model, each 1°C increase in average race-day temperature is associated with **~1.49 minutes** slower finishing time.
- **Non-linear optimum:** a quadratic model places the best performance near **10.6°C**, matching the descriptive result that the 8–12°C range is fastest.
- **Unequal sensitivity:** slower runners (4h+) are more sensitive to heat than faster runners (sub-3h).
- The models explain little of the individual-level variance (R² < 0.01), as expected when age, training and pacing are not observed; the estimates are best read as field-wide summaries, not causal effects.

![Average finishing time by temperature bin](report/figures/temperature_bin_finish_time.png)

![Estimated temperature sensitivity by runner group](report/figures/temperature_sensitivity_by_group.png)

## Pipeline

```text
raw CSV ──► HDFS ──► 01_etl.py (Spark: clean, validate, join) ──► Parquet
                                                                   │
                         02_eda.py (grouped summaries) ◄───────────┤
                         03_modeling.py (Spark MLlib regression) ◄─┘
                                       │
                         04_make_figures.py (Matplotlib) ──► figures
```

| Script | Purpose |
|---|---|
| `scripts/00_profile_data.py` | Profile raw data quality and unusual values |
| `scripts/01_etl.py` | Clean runner records, parse finish times, join weather by year, write Parquet |
| `scripts/02_eda.py` | Descriptive summary tables by year, gender, temperature bin and performance group |
| `scripts/03_modeling.py` | Linear and quadratic temperature models and grouped slope comparisons (`pyspark.ml`) |
| `scripts/04_make_figures.py` | Report-ready Matplotlib figures from the saved summary tables |

All reported coefficients, grouped means and figures come from the same intermediate Parquet dataset, so every team member reproduces identical results.

## Run Locally

Requires Python 3, Java, and Spark (`spark-submit` on your `PATH`), plus `pyspark`, `numpy`, `pandas`, `matplotlib` and `seaborn`.

```bash
spark-submit --master "local[*]" scripts/01_etl.py
spark-submit --master "local[*]" scripts/02_eda.py
spark-submit --master "local[*]" scripts/03_modeling.py
python3 scripts/04_make_figures.py
```

Each script uses the repository's default paths (`src/` → `outputs/processed/` → `outputs/tables/` → `outputs/figures/`); run any script with `--help` to override them.

## Run on a Hadoop/Spark VM

The team ran the pipeline on a Linux VM with HDFS and YARN. Copy the config template and fill in your own VM settings (the real file is git-ignored):

```bash
cp config/utm_vm.env.example config/utm_vm.env
./scripts/vm_pipeline.sh all   # sync → start services → HDFS upload → ETL → EDA → modeling → pull results
python3 scripts/04_make_figures.py
```

Individual stages: `sync`, `start-services`, `install-python-deps`, `hdfs-setup`, `etl`, `eda`, `modeling`, `pull-outputs`.

## Data

- **Runner results and weather:** [Berlin Marathons Data (1974–2023)](https://www.kaggle.com/datasets/aiaiaidavid/berlin-marathons-data) by Aiai David, CC0 1.0 (public domain). This repository includes the 1974–2019 subset used in the analysis under `src/`. The results file contains no runner names.

## Report

The paper source is [`report/report.qmd`](report/report.qmd) (Quarto, IEEE conference format). Render it with `quarto render report/report.qmd`. Cited articles are listed in `report/references.bib` and are not redistributed here.
