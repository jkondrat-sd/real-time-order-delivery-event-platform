#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for MinIO to be available..."
until curl -sS http://localhost:9000/minio/health/ready >/dev/null 2>&1; do
  sleep 1
done

if ! command -v mc >/dev/null 2>&1; then
  echo "Please install 'mc' (MinIO client) to run this script, or create bucket 'iceberg-warehouse' manually."
  exit 1
fi

MC_ALIAS=localminio
mc alias set $MC_ALIAS http://localhost:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin}
mc mb -p $MC_ALIAS/iceberg-warehouse || true
echo "Created bucket iceberg-warehouse (or it already existed)."
