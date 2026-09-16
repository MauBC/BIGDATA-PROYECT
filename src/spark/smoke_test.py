from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATASET = (
    "gs://rawg-bigdata-86233853262/"
    "processed/videogames_clean.parquet"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RAWG-Spark-Smoke-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 90)
    print("SPARK - PRUEBA DE LECTURA DESDE GOOGLE CLOUD STORAGE")
    print("=" * 90)

    print(f"\nDataset: {DATASET}")

    df = spark.read.parquet(DATASET)

    print(f"\nParticiones Spark: {df.rdd.getNumPartitions()}")
    print(f"Columnas: {len(df.columns)}")

    print("\nCalculando métricas...")

    metrics = (
        df.agg(
            F.count("*").alias("total_registros"),
            F.countDistinct("id").alias("ids_unicos"),
            F.min("release_year").alias("anio_minimo"),
            F.max("release_year").alias("anio_maximo"),
        )
        .collect()[0]
    )

    total = metrics["total_registros"]
    unique_ids = metrics["ids_unicos"]
    min_year = metrics["anio_minimo"]
    max_year = metrics["anio_maximo"]

    print(f"\nFilas       : {total:,}")
    print(f"IDs unicos  : {unique_ids:,}")
    print(f"Anio minimo : {min_year}")
    print(f"Anio maximo : {max_year}")

    print("\nPrimeras 5 filas:")

    (
        df.select(
            "id",
            "name",
            "release_year",
            "genres",
            "platforms",
        )
        .show(
            5,
            truncate=50,
        )
    )

    print("=" * 90)

    if (
        total == 899_585
        and unique_ids == 899_585
        and min_year == 1954
        and max_year == 2033
    ):
        print("[OK] SPARK LEE CORRECTAMENTE EL PARQUET DESDE GCS")
    else:
        print("[ERROR] LOS RESULTADOS NO COINCIDEN")
        raise SystemExit(1)

    print("=" * 90)

    spark.stop()


if __name__ == "__main__":
    main()
