#!/usr/bin/env bash
# Usage: ./scripts/install_flink_jar.sh <jar-url>
set -euo pipefail
if [ $# -ne 1 ]; then
  echo "Usage: $0 <jar-url>"
  exit 1
fi
JAR_URL=$1
JAR_NAME=$(basename "$JAR_URL")
TMP="/tmp/$JAR_NAME"

echo "Downloading $JAR_URL to $TMP"
curl -fSL "$JAR_URL" -o "$TMP"

echo "Copying $JAR_NAME to Flink JobManager and TaskManager"
docker cp "$TMP" local-flink-jobmanager-1:/opt/flink/lib/
docker cp "$TMP" local-flink-taskmanager-1:/opt/flink/lib/

echo "Restarting Flink JobManager and TaskManager"
docker restart local-flink-jobmanager-1 local-flink-taskmanager-1

echo "Done."
