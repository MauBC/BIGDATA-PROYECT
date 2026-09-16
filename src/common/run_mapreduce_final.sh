#!/bin/bash
set -euo pipefail

JAR="/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar"
INPUT="/raw/videogames_data.csv"
BASE="/mapreduce_final"

echo "============================================================"
echo "MAPREDUCE FINAL - 1 REDUCER"
echo "============================================================"

hdfs dfs -mkdir -p "$BASE"

hdfs dfs -rm -r -f "$BASE/wordmean"
hdfs dfs -rm -r -f "$BASE/wordmedian"
hdfs dfs -rm -r -f "$BASE/wordstandarddeviation"

echo
echo "============================================================"
echo "1. WORDMEAN"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordmean \
  -Dmapreduce.job.reduces=1 \
  "$INPUT" \
  "$BASE/wordmean"

END=$(date +%s)

echo "Tiempo WORDMEAN: $((END - START)) segundos"
echo
echo "Salida HDFS:"
hdfs dfs -cat "$BASE/wordmean/part-r-00000"

echo
echo "============================================================"
echo "2. WORDMEDIAN"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordmedian \
  -Dmapreduce.job.reduces=1 \
  "$INPUT" \
  "$BASE/wordmedian"

END=$(date +%s)

echo "Tiempo WORDMEDIAN: $((END - START)) segundos"
echo
echo "Primeras frecuencias:"
hdfs dfs -cat "$BASE/wordmedian/part-r-00000" | head -n 20

echo
echo "============================================================"
echo "3. WORDSTANDARDDEVIATION"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordstandarddeviation \
  -Dmapreduce.job.reduces=1 \
  "$INPUT" \
  "$BASE/wordstandarddeviation"

END=$(date +%s)

echo "Tiempo WORDSTANDARDDEVIATION: $((END - START)) segundos"
echo
echo "Salida HDFS:"
hdfs dfs -cat "$BASE/wordstandarddeviation/part-r-00000"

echo
echo "============================================================"
echo "RESULTADOS FINALES EN HDFS"
echo "============================================================"

hdfs dfs -ls -R "$BASE"

echo
echo "============================================================"
echo "MAPREDUCE FINALIZADO CORRECTAMENTE"
echo "============================================================"
