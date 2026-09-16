#!/bin/bash
set -euo pipefail

echo "=================================================="
echo "CREANDO DIRECTORIO HDFS"
echo "=================================================="

hdfs dfs -mkdir -p /raw

echo
echo "=================================================="
echo "COPIANDO GCS -> HDFS CON DISTCP"
echo "=================================================="

hadoop distcp -overwrite \
  gs://rawg-bigdata-86233853262/raw/videogames_data.csv \
  hdfs:///raw/

echo
echo "=================================================="
echo "CONTENIDO DE /raw"
echo "=================================================="

hdfs dfs -ls -h /raw

echo
echo "=================================================="
echo "TAMANO EXACTO EN HDFS"
echo "=================================================="

hdfs dfs -stat "%b bytes - %n" /raw/videogames_data.csv

echo
echo "=================================================="
echo "REPLICACION HDFS"
echo "=================================================="

hdfs dfs -stat "replication=%r block_size=%o bytes=%b file=%n" /raw/videogames_data.csv
