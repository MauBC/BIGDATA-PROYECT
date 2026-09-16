#!/bin/bash
set -euo pipefail

INPUT="/mapreduce_final/wordmedian/part-r-*"
TMP="/tmp/rawg_word_lengths.tsv"

echo "============================================================"
echo "RESUMEN ESTADISTICO DESDE RESULTADO MAPREDUCE"
echo "============================================================"

hdfs dfs -cat $INPUT | sort -n -k1,1 > "$TMP"

awk '
{
    len[NR] = $1
    freq[NR] = $2

    total += $2
    sum += $1 * $2
    sumsq += ($1 * $1) * $2
}
END {
    target1 = int((total + 1) / 2)
    target2 = int((total + 2) / 2)

    cumulative = 0
    median1 = -1
    median2 = -1

    for (i = 1; i <= NR; i++) {
        previous = cumulative
        cumulative += freq[i]

        if (median1 < 0 && target1 > previous && target1 <= cumulative) {
            median1 = len[i]
        }

        if (median2 < 0 && target2 > previous && target2 <= cumulative) {
            median2 = len[i]
        }
    }

    mean = sum / total
    median = (median1 + median2) / 2.0
    variance = (sumsq / total) - (mean * mean)
    stddev = sqrt(variance)

    printf "Total tokens              : %.0f\n", total
    printf "Suma longitudes           : %.0f\n", sum
    printf "Media                     : %.12f\n", mean
    printf "Mediana                   : %.4f\n", median
    printf "Desviacion estandar       : %.12f\n", stddev
    printf "Suma cuadrados corregida  : %.0f\n", sumsq
}
' "$TMP"

echo
echo "============================================================"
echo "WORDMEAN ORIGINAL"
echo "============================================================"

hdfs dfs -cat /mapreduce_final/wordmean/part-r-00000

echo
echo "============================================================"
echo "STDDEV ORIGINAL DE HADOOP"
echo "============================================================"

hdfs dfs -cat /mapreduce_final/wordstandarddeviation/part-r-00000
