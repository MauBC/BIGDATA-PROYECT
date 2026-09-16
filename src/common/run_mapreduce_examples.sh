#!/bin/bash
set -euo pipefail

JAR="/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar"
INPUT="/raw/videogames_data.csv"

echo "============================================================"
echo "PREPARANDO DIRECTORIOS"
echo "============================================================"

hdfs dfs -mkdir -p /mapreduce

hdfs dfs -rm -r -f /mapreduce/wordmean
hdfs dfs -rm -r -f /mapreduce/wordmedian
hdfs dfs -rm -r -f /mapreduce/wordstandarddeviation

echo
echo "============================================================"
echo "1. WORDMEAN"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordmean \
  "$INPUT" \
  /mapreduce/wordmean

END=$(date +%s)

echo "Tiempo WORDMEAN: $((END - START)) segundos"

echo
echo "Resultado WORDMEAN:"
hdfs dfs -cat /mapreduce/wordmean/part-r-00000

echo
echo "============================================================"
echo "2. WORDMEDIAN"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordmedian \
  "$INPUT" \
  /mapreduce/wordmedian

END=$(date +%s)

echo "Tiempo WORDMEDIAN: $((END - START)) segundos"

echo
echo "Resultado WORDMEDIAN:"
hdfs dfs -cat /mapreduce/wordmedian/part-r-00000

echo
echo "============================================================"
echo "3. WORDSTANDARDDEVIATION"
echo "============================================================"

START=$(date +%s)

hadoop jar "$JAR" wordstandarddeviation \
  "$INPUT" \
  /mapreduce/wordstandarddeviation

END=$(date +%s)

echo "Tiempo WORDSTANDARDDEVIATION: $((END - START)) segundos"

echo
echo "Resultado WORDSTANDARDDEVIATION:"
hdfs dfs -cat /mapreduce/wordstandarddeviation/part-r-00000

echo
echo "============================================================"
echo "ARCHIVOS GENERADOS EN HDFS"
echo "============================================================"

hdfs dfs -ls -R /mapreduce

echo
echo "============================================================"
echo "MAPREDUCE FINALIZADO"
echo "============================================================"
