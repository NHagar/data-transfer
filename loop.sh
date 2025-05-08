#!/usr/bin/env bash
set -euo pipefail

# List the datasets you want to mirror
DATASETS=(
  nhagar/fineweb_urls
)

# Where to store the temporary downloads
LOCAL_ROOT="/mnt/nvme/hf"

# S3 bucket that will hold everything
BUCKET="hf-datasets-nh"

# Tune the AWS CLI *once* (no need to repeat inside the loop)
aws configure set default.s3.max_concurrent_requests 64
aws configure set default.s3.multipart_chunksize 64MB

for REPO in "${DATASETS[@]}"; do
  # Split "user/dataset" into USER and DSNAME
  IFS='/' read -r USER DSNAME <<< "$REPO"

  # Local and remote destinations
  LOCAL_DIR="${LOCAL_ROOT}/${DSNAME}"
  S3_PREFIX="s3://${BUCKET}/${DSNAME}/"

  echo "=== Mirroring ${REPO} → ${S3_PREFIX}"

  # 1) Download from the Hub
  uv run main.py "$REPO" "$LOCAL_DIR"

  # 2) Push to S3
  aws s3 sync "${LOCAL_DIR}" "${S3_PREFIX}" --exclude ".cache/*" --exclude ".cache/**" --no-follow-symlinks

  # 3) Clean up local storage
  rm -rf "${LOCAL_DIR}"
done
