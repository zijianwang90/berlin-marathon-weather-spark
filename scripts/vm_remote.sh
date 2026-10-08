#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
HDFS_USER_NAME="${HDFS_USER_NAME:-${USER}}"
HDFS_INPUT_DIR="${HDFS_INPUT_DIR:-/user/${HDFS_USER_NAME}/berlin_marathon_input}"
HDFS_OUTPUT_DIR="${HDFS_OUTPUT_DIR:-/user/${HDFS_USER_NAME}/berlin_marathon_output}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-$(command -v spark-submit || true)}"
# Ask the active Hadoop config for fs.defaultFS so the same script works even if
# a teammate's VM uses a different NameNode host or port.
FS_DEFAULT="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"

if [[ -z "${SPARK_SUBMIT_BIN}" ]]; then
  SPARK_SUBMIT_BIN="/home/hadoop/spark-3.5.5-bin-hadoop3/bin/spark-submit"
fi

usage() {
  cat <<EOF
Usage: ./scripts/vm_remote.sh <command>

Commands:
  start-services
  install-python-deps
  hdfs-setup
  etl
  eda
  modeling
EOF
}

spark_submit() {
  local script_path="$1"
  shift

  # Centralize spark-submit flags here so ETL / EDA / modeling stay consistent.
  local spark_args=(
    "${SPARK_SUBMIT_BIN}"
    --master "${SPARK_MASTER}"
  )

  if [[ -n "${FS_DEFAULT}" ]]; then
    spark_args+=(--conf "spark.hadoop.fs.defaultFS=${FS_DEFAULT}")
  fi

  spark_args+=("${script_path}" "$@")
  "${spark_args[@]}"
}

cmd_start_services() {
  echo "[1/4] Starting HDFS services"
  "${HADOOP_HOME}/sbin/start-dfs.sh" || true

  echo "[2/4] Starting YARN services"
  "${HADOOP_HOME}/sbin/start-yarn.sh" || true

  if [[ -x "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" ]]; then
    echo "[3/4] Starting MapReduce history server"
    "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" start historyserver || true
  else
    echo "[3/4] History server script not found, skipping"
  fi

  echo "[4/4] Leaving HDFS safe mode if needed"
  hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true

  echo
  echo "Running JVM services:"
  jps
}

cmd_install_python_deps() {
  echo "[1/3] Checking Python"
  "${PYTHON_BIN}" --version

  echo "[2/3] Checking pip"
  if ! "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
    echo "pip is not available for ${PYTHON_BIN}." >&2
    echo "Install it first, for example: sudo apt-get update && sudo apt-get install -y python3-pip" >&2
    exit 1
  fi

  echo "[3/3] Installing Python dependencies required by pyspark.ml"
  # numpy is required because pyspark.ml depends on it for vector handling.
  "${PYTHON_BIN}" -m pip install --user numpy

  echo
  echo "Installed numpy for ${PYTHON_BIN}"
}

cmd_hdfs_setup() {
  local results_csv="${PROJECT_ROOT}/src/Berlin_Marathon_data_1974_2019.csv"
  local weather_csv="${PROJECT_ROOT}/src/Berlin_Marathon_weather_data_since_1974.csv"

  echo "[1/3] Creating HDFS input directory ${HDFS_INPUT_DIR}"
  hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"

  echo "[2/3] Uploading raw results CSV"
  hdfs dfs -put -f "${results_csv}" "${HDFS_INPUT_DIR}/"

  echo "[3/3] Uploading weather CSV"
  hdfs dfs -put -f "${weather_csv}" "${HDFS_INPUT_DIR}/"

  echo
  echo "HDFS input directory contents:"
  hdfs dfs -ls "${HDFS_INPUT_DIR}"
}

cmd_etl() {
  spark_submit \
    "${PROJECT_ROOT}/scripts/01_etl.py" \
    --results "${HDFS_INPUT_DIR}/Berlin_Marathon_data_1974_2019.csv" \
    --weather "${HDFS_INPUT_DIR}/Berlin_Marathon_weather_data_since_1974.csv" \
    --output "${HDFS_OUTPUT_DIR}/marathon_joined"

  echo
  echo "HDFS output directory contents:"
  hdfs dfs -ls "${HDFS_OUTPUT_DIR}"
}

cmd_eda() {
  mkdir -p "${PROJECT_ROOT}/outputs/tables/eda"
  spark_submit \
    "${PROJECT_ROOT}/scripts/02_eda.py" \
    --input "${HDFS_OUTPUT_DIR}/marathon_joined" \
    --output-dir "${PROJECT_ROOT}/outputs/tables/eda"

  echo
  echo "EDA tables written under ${PROJECT_ROOT}/outputs/tables/eda"
}

cmd_modeling() {
  mkdir -p "${PROJECT_ROOT}/outputs/tables/modeling"
  # Modeling writes tables locally on the VM because those are later pulled
  # back to the host for figure generation and report writing.
  spark_submit \
    "${PROJECT_ROOT}/scripts/03_modeling.py" \
    --input "${HDFS_OUTPUT_DIR}/marathon_joined" \
    --output-dir "${PROJECT_ROOT}/outputs/tables/modeling"

  echo
  echo "Modeling tables written under ${PROJECT_ROOT}/outputs/tables/modeling"
}

main() {
  local command="${1:-}"
  case "${command}" in
    start-services) cmd_start_services ;;
    install-python-deps) cmd_install_python_deps ;;
    hdfs-setup) cmd_hdfs_setup ;;
    etl) cmd_etl ;;
    eda) cmd_eda ;;
    modeling) cmd_modeling ;;
    *) usage; exit 1 ;;
  esac
}

main "$@"
