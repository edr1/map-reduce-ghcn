#!/usr/bin/env bash
set -Eeuo pipefail

JAR="${JAR:-target/ghcnd-mapreduce-1.0.0.jar}"
INPUT="${INPUT:-s3://CAMBIAR-BUCKET/noaa-ghcn/input/}"
OUTPUT_BASE="${OUTPUT_BASE:-s3://CAMBIAR-BUCKET/noaa-ghcn/output}"
MARCA="$(date +%Y%m%d-%H%M%S)"

hadoop jar "$JAR" avg-tmax "$INPUT" "$OUTPUT_BASE/avg-tmax-$MARCA"
hadoop jar "$JAR" monthly-prcp "$INPUT" "$OUTPUT_BASE/monthly-prcp-$MARCA"
hadoop jar "$JAR" annual-max "$INPUT" "$OUTPUT_BASE/annual-max-$MARCA"

echo "Resultados creados bajo $OUTPUT_BASE con marca $MARCA"
