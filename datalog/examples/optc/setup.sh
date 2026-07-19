#!/usr/bin/env bash
#
# Downloads a starter slice of the DARPA OpTC dataset (host bucket
# AIA-201-225 for red-team day 1, 23 Sep 2019) into data/. The datalog
# jsonfacts loader reads .gz sources directly, so the files stay
# compressed on disk (~2.2 GB total).
#
# Needs gdown (https://github.com/wkentaro/gdown), e.g. pipx install gdown;
# without it, download the files by browser or rclone as described in
# README.md and place them in data/ yourself.
#
# OpTC data is released by DARPA / Five Directions; see
# https://github.com/FiveDirections/OpTC-data for terms and documentation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

# Starter slice: ecar/evaluation/23Sep19-red/AIA-201-225/ — the day-1
# initial-compromise host (SysClient0201) plus its 24 bucket neighbors.
# Google Drive file IDs verified 2026-07-18.
declare -A STARTER_FILES=(
    ["AIA-201-225.ecar-2019-12-08T11-05-10.046.json.gz"]="1pJLxJsDV8sngiedbfVajMetczIgM3PQd"
    ["AIA-201-225.ecar-last.json.gz"]="1HFSyvmgH0jvdnnnTdKfWRjZYOrLWoIkv"
)

mkdir -p "$DATA_DIR"

for name in "${!STARTER_FILES[@]}"; do
    if [ -f "$DATA_DIR/$name" ]; then
        echo "Already present: $name"
        continue
    fi
    if ! command -v gdown >/dev/null 2>&1; then
        echo "error: gdown is not installed and $name is missing." >&2
        echo "Install gdown (pipx install gdown) or download it by" >&2
        echo "browser/rclone as described in README.md into data/." >&2
        exit 1
    fi
    echo "Downloading $name ..."
    gdown --output "$DATA_DIR/$name" "${STARTER_FILES[$name]}"
done

echo "Verifying gzip integrity ..."
for name in "${!STARTER_FILES[@]}"; do
    gunzip -t "$DATA_DIR/$name"
    echo "  ok: $name"
done

echo "Done. Data in $DATA_DIR:"
ls -lh "$DATA_DIR"/*.json.gz
