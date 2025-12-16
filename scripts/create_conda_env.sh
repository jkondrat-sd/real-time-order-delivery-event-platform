#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="environment.yml"
ENV_NAME="rtod-env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

echo "Creating Conda environment '$ENV_NAME' from $ENV_FILE..."
conda env create -f "$ENV_FILE" -n "$ENV_NAME" || echo "Environment may already exist. To recreate: conda env remove -n $ENV_NAME && conda env create -f $ENV_FILE -n $ENV_NAME"

echo "To activate: conda activate $ENV_NAME"
