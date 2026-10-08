#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/utm_vm_common.sh"

SYNC_ARCHIVE_NAME=".vm_sync_bundle.tar.gz"

usage() {
  cat <<EOF
Usage: ./scripts/vm_pipeline.sh <command>

Commands:
  sync
  start-services
  install-python-deps
  hdfs-setup
  etl
  eda
  modeling
  pull-outputs
  all
EOF
}

sync_project() {
  # Keep the remote layout predictable so every teammate runs the same paths.
  echo "[1/3] Creating remote project directories"
  expect_ssh "mkdir -p '${UTM_VM_PROJECT_DIR}' '${UTM_VM_PROJECT_DIR}/outputs/processed' '${UTM_VM_PROJECT_DIR}/outputs/tables' '${UTM_VM_PROJECT_DIR}/outputs/figures'"

  echo "[2/3] Packing project files for sync"
  rm -f "${PROJECT_ROOT}/${SYNC_ARCHIVE_NAME}"
  COPYFILE_DISABLE=1 tar \
    --exclude=".DS_Store" \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude=".venv*" \
    --exclude=".mypy_cache" \
    --exclude=".pytest_cache" \
    --exclude=".quarto" \
    --exclude=".mplconfig" \
    --exclude="outputs" \
    -czf "${PROJECT_ROOT}/${SYNC_ARCHIVE_NAME}" \
    -C "${PROJECT_ROOT}" \
    src scripts docs report README.md .gitignore

  echo "[2/3] Copying project files to ${UTM_VM_USER}@${UTM_VM_HOST}:${UTM_VM_PROJECT_DIR}"
  expect_scp_to "${PROJECT_ROOT}/${SYNC_ARCHIVE_NAME}" "${UTM_VM_PROJECT_DIR}/"
  expect_ssh "cd '${UTM_VM_PROJECT_DIR}' && tar -xzf '${SYNC_ARCHIVE_NAME}' && rm -f '${SYNC_ARCHIVE_NAME}'"
  rm -f "${PROJECT_ROOT}/${SYNC_ARCHIVE_NAME}"

  echo "[3/3] Ensuring remote scripts are executable"
  expect_ssh "chmod +x '${UTM_VM_PROJECT_DIR}/scripts/'*.sh"

  echo
  echo "Remote project synced to ${UTM_VM_USER}@${UTM_VM_HOST}:${UTM_VM_PROJECT_DIR}"
}

run_remote() {
  local remote_command="$1"
  # All remote stage logic lives in vm_remote.sh so the local wrapper stays thin.
  expect_ssh "cd '${UTM_VM_PROJECT_DIR}' && ./scripts/vm_remote.sh ${remote_command}"
}

install_python_deps() {
  # Some teammate VMs do not have pip preinstalled. Bootstrap it in user space
  # so the shared pyspark.ml modeling path can rely on numpy being present
  # without assuming sudo or apt are usable.
  expect_ssh "if ! python3 -m pip --version >/dev/null 2>&1; then curl -fsSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && python3 /tmp/get-pip.py --user && rm -f /tmp/get-pip.py; fi"
  run_remote install-python-deps
}

pull_outputs() {
  mkdir -p "${PROJECT_ROOT}/outputs"

  # Only pull report-facing outputs back to local; raw parquet stays in HDFS.
  echo "[1/2] Pulling EDA tables from remote VM"
  expect_scp_from "${UTM_VM_PROJECT_DIR}/outputs/tables/eda" "${PROJECT_ROOT}/outputs/tables/"

  echo "[2/2] Pulling modeling tables from remote VM"
  expect_scp_from "${UTM_VM_PROJECT_DIR}/outputs/tables/modeling" "${PROJECT_ROOT}/outputs/tables/"

  echo
  echo "Pulled remote outputs into ${PROJECT_ROOT}/outputs/tables"
}

main() {
  local command="${1:-}"
  case "${command}" in
    sync) sync_project ;;
    start-services) run_remote start-services ;;
    install-python-deps) install_python_deps ;;
    hdfs-setup) run_remote hdfs-setup ;;
    etl) run_remote etl ;;
    eda) run_remote eda ;;
    modeling) run_remote modeling ;;
    pull-outputs) pull_outputs ;;
    all)
      # Standard reproducibility path for teammates and grading demos.
      sync_project
      run_remote start-services
      install_python_deps
      run_remote hdfs-setup
      run_remote etl
      run_remote eda
      run_remote modeling
      pull_outputs
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
