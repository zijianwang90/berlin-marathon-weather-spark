#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UTM_VM_CONFIG="${UTM_VM_CONFIG:-${PROJECT_ROOT}/config/utm_vm.env}"

if [[ -f "${UTM_VM_CONFIG}" ]]; then
  # Load per-user VM settings without forcing every teammate to edit scripts.
  # shellcheck disable=SC1090
  source "${UTM_VM_CONFIG}"
fi

UTM_VM_HOST="${UTM_VM_HOST:-}"
UTM_VM_USER="${UTM_VM_USER:-}"
UTM_VM_PASSWORD="${UTM_VM_PASSWORD:-}"
UTM_VM_PROJECT_DIR="${UTM_VM_PROJECT_DIR:-/home/hadoop/berlin-marathon-weather-spark}"

if [[ -z "${UTM_VM_HOST}" || -z "${UTM_VM_USER}" || -z "${UTM_VM_PASSWORD}" ]]; then
  cat >&2 <<EOF
Missing UTM VM connection settings.

Create ${UTM_VM_CONFIG} from config/utm_vm.env.example and fill in:
  UTM_VM_HOST
  UTM_VM_USER
  UTM_VM_PASSWORD

Optional:
  UTM_VM_PROJECT_DIR
EOF
  exit 1
fi

expect_ssh() {
  local remote_cmd="$1"
  # Escape single quotes so arbitrary bash snippets can be passed safely
  # through ssh -> bash -ic without being truncated.
  local escaped_cmd="${remote_cmd//\'/\'\\\'\'}"

expect <<EOF
set timeout -1
spawn ssh -tt -o StrictHostKeyChecking=no ${UTM_VM_USER}@${UTM_VM_HOST} "bash -ic '$escaped_cmd'"
expect {
  -re "(?i)password:" {
    send "${UTM_VM_PASSWORD}\r"
    exp_continue
  }
  eof
}
catch wait result
exit [lindex \$result 3]
EOF
}

expect_scp_to() {
  local source_path="$1"
  local remote_path="$2"

  expect <<EOF
set timeout -1
spawn scp -o StrictHostKeyChecking=no -r "$source_path" "${UTM_VM_USER}@${UTM_VM_HOST}:$remote_path"
expect {
  -re "(?i)password:" {
    send "${UTM_VM_PASSWORD}\r"
    exp_continue
  }
  eof
}
catch wait result
exit [lindex \$result 3]
EOF
}

expect_scp_from() {
  local remote_path="$1"
  local local_path="$2"

  expect <<EOF
set timeout -1
spawn scp -o StrictHostKeyChecking=no -r "${UTM_VM_USER}@${UTM_VM_HOST}:$remote_path" "$local_path"
expect {
  -re "(?i)password:" {
    send "${UTM_VM_PASSWORD}\r"
    exp_continue
  }
  eof
}
catch wait result
exit [lindex \$result 3]
EOF
}
