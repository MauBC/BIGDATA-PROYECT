from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DATASET = (
    "gs://rawg-bigdata-86233853262/"
    "processed/videogames_clean.parquet"
)

OUTPUT_BASE = (
    "gs://rawg-bigdata-86233853262/"
    "results/spark"
)


def separator(title: str) -> None:
    print()
    print("=" * 110)
    print(title)
    print("=" * 110)


def explode_dimension(
    df: DataFrame,
    column: str,
    extra_columns: list[str] | None = None,
) -> DataFrame:

    columns = ["id", column]

    if extra_columns:
        columns.extend(extra_columns)

    work = (
        df.select(*columns)
        .filter(F.col(column).isNotNull())
        .withColumn(
            column,
            F.explode(
                F.split(
                    F.col(column),
                    r"\|",
                )
            ),
        )
        .withColumn(
            column,
            F.trim(F.col(column)),
        )
        .filter(
            F.col(column).isNotNull()
            & (F.col(column) != "")
        )
    )

    return work


def execute_query(
    query_id: str,
    title: str,
    result: DataFrame,
    output_name: str,
) -> dict:

    separator(f"{query_id} - {title}")

    result = result.cache()

    start = time.perf_counter()

    rows = result.count()

    elapsed = time.perf_counter() - start

    output_path = f"{OUTPUT_BASE}/{output_name}"

    (
        result
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    result.show(
        n=max(rows, 20),
        truncate=False,
    )

    print()
    print(f"Filas resultado : {rows}")
    print(f"Tiempo          : {elapsed:.4f} segundos")
    print(f"Salida GCS      : {output_path}")

    result.unpersist()

    return {
        "query_id": query_id,
        "consulta": title,
        "filas_resultado": int(rows),
        "tiempo_segundos": float(elapsed),
        "archivo": output_path,
    }


def main() -> None:

    spark = (
        SparkSession.builder
        .appName("RAWG-Spark-10-Consultas")
        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    separator(
        "SPARK - 10 CONSULTAS OFICIALES RAWG"
    )

    print(f"\nDataset: {DATASET}")
    print(f"Resultados: {OUTPUT_BASE}")

    df = spark.read.parquet(DATASET)

    print(
        f"Particiones iniciales: "
        f"{df.rdd.getNumPartitions()}"
    )

    print(
        f"Columnas: {len(df.columns)}"
    )

    timings = []

    # =========================================================
    # Q01
    # =========================================================

    q01 = df.agg(
        F.count("*")
        .alias("total_registros"),

        F.countDistinct("id")
        .alias("ids_unicos"),

        F.min("release_year")
        .alias("anio_minimo"),

        F.max("release_year")
        .alias("anio_maximo"),

        F.count("rating")
        .alias("juegos_con_rating"),

        F.count("metacritic")
        .alias("juegos_con_metacritic"),

        F.count("genres")
        .alias("juegos_con_genero"),

        F.count("platforms")
        .alias("juegos_con_plataforma"),
    )

    timings.append(
        execute_query(
            "Q01",
            "Resumen general y cobertura",
            q01,
            "q01_resumen_general",
        )
    )

    # =========================================================
    # Q02
    # =========================================================

    q02 = (
        df
        .filter(
            F.col("release_year").isNotNull()
        )
        .groupBy("release_year")
        .agg(
            F.countDistinct("id")
            .alias("videojuegos")
        )
        .orderBy(
            F.col("release_year").asc()
        )
    )

    timings.append(
        execute_query(
            "Q02",
            "Videojuegos lanzados por año",
            q02,
            "q02_videojuegos_por_anio",
        )
    )

    # =========================================================
    # Q03
    # =========================================================

    genres = explode_dimension(
        df,
        "genres",
    )

    q03 = (
        genres
        .groupBy("genres")
        .agg(
            F.countDistinct("id")
            .alias("videojuegos")
        )
        .orderBy(
            F.col("videojuegos").desc(),
            F.col("genres").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q03",
            "Top 20 generos por cantidad de videojuegos",
            q03,
            "q03_top_generos",
        )
    )

    # =========================================================
    # Q04
    # =========================================================

    platforms = explode_dimension(
        df,
        "platforms",
    )

    q04 = (
        platforms
        .groupBy("platforms")
        .agg(
            F.countDistinct("id")
            .alias("videojuegos")
        )
        .orderBy(
            F.col("videojuegos").desc(),
            F.col("platforms").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q04",
            "Top 20 plataformas por cantidad de videojuegos",
            q04,
            "q04_top_plataformas",
        )
    )

    # =========================================================
    # Q05
    # =========================================================

    developers = explode_dimension(
        df,
        "developers",
    )

    q05 = (
        developers
        .groupBy("developers")
        .agg(
            F.countDistinct("id")
            .alias("videojuegos")
        )
        .orderBy(
            F.col("videojuegos").desc(),
            F.col("developers").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q05",
            "Top 20 desarrolladores por cantidad de videojuegos",
            q05,
            "q05_top_desarrolladores",
        )
    )

    # =========================================================
    # Q06
    # =========================================================

    rating_genres = (
        explode_dimension(
            df,
            "genres",
            extra_columns=["rating"],
        )
        .filter(
            F.col("rating").isNotNull()
        )
    )

    q06 = (
        rating_genres
        .groupBy("genres")
        .agg(
            F.countDistinct("id")
            .alias("juegos_con_rating"),

            F.bround(
                F.avg("rating"),
                4,
            ).alias("rating_promedio"),

            F.bround(
                F.median("rating"),
                4,
            ).alias("rating_mediano"),
        )
        .filter(
            F.col("juegos_con_rating") >= 50
        )
        .orderBy(
            F.col("rating_promedio").desc(),
            F.col("genres").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q06",
            "Rating promedio por genero",
            q06,
            "q06_rating_por_genero",
        )
    )

    # =========================================================
    # Q07
    # =========================================================

    metacritic_genres = (
        explode_dimension(
            df,
            "genres",
            extra_columns=["metacritic"],
        )
        .filter(
            F.col("metacritic").isNotNull()
        )
    )

    q07 = (
        metacritic_genres
        .groupBy("genres")
        .agg(
            F.countDistinct("id")
            .alias(
                "juegos_con_metacritic"
            ),

            F.bround(
                F.avg("metacritic"),
                4,
            ).alias(
                "metacritic_promedio"
            ),

            F.bround(
                F.median("metacritic"),
                4,
            ).alias(
                "metacritic_mediano"
            ),
        )
        .filter(
            F.col(
                "juegos_con_metacritic"
            ) >= 20
        )
        .orderBy(
            F.col(
                "metacritic_promedio"
            ).desc(),
            F.col("genres").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q07",
            "Metacritic promedio por genero",
            q07,
            "q07_metacritic_por_genero",
        )
    )

    # =========================================================
    # Q08
    # =========================================================

    q08 = (
        df
        .filter(
            F.col(
                "ratings_count"
            ).isNotNull()
        )
        .select(
            "id",
            "name",
            "release_year",
            "rating",
            "ratings_count",
            "reviews_count",
            "genres",
        )
        .orderBy(
            F.col(
                "ratings_count"
            ).desc()
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q08",
            "Top 20 videojuegos por cantidad de ratings",
            q08,
            "q08_top_videojuegos_ratings",
        )
    )

    # =========================================================
    # Q09
    # =========================================================

    playtime_genres = (
        explode_dimension(
            df,
            "genres",
            extra_columns=["playtime"],
        )
        .filter(
            F.col("playtime").isNotNull()
        )
    )

    q09 = (
        playtime_genres
        .groupBy("genres")
        .agg(
            F.countDistinct("id")
            .alias("juegos_con_playtime"),

            F.bround(
                F.avg("playtime"),
                4,
            ).alias("playtime_promedio"),

            F.bround(
                F.median("playtime"),
                4,
            ).alias("playtime_mediano"),

            F.max("playtime")
            .alias("playtime_maximo"),
        )
        .filter(
            F.col(
                "juegos_con_playtime"
            ) >= 30
        )
        .orderBy(
            F.col(
                "playtime_promedio"
            ).desc(),
            F.col("genres").asc(),
        )
        .limit(20)
    )

    timings.append(
        execute_query(
            "Q09",
            "Playtime promedio por genero",
            q09,
            "q09_playtime_por_genero",
        )
    )

    # =========================================================
    # Q10
    # =========================================================

    q10 = (
        df
        .filter(
            F.col("release_year").isNotNull()
        )
        .groupBy("release_year")
        .agg(
            F.countDistinct("id")
            .alias("videojuegos"),

            F.count("rating")
            .alias("juegos_con_rating"),

            F.bround(
                F.avg("rating"),
                4,
            ).alias("rating_promedio"),

            F.count("metacritic")
            .alias(
                "juegos_con_metacritic"
            ),

            F.bround(
                F.avg("metacritic"),
                4,
            ).alias(
                "metacritic_promedio"
            ),

            F.coalesce(
                F.sum("ratings_count"),
                F.lit(0),
            ).alias("total_ratings"),
        )
        .orderBy(
            F.col("release_year").asc()
        )
    )

    timings.append(
        execute_query(
            "Q10",
            "Evolucion anual de videojuegos y valoraciones",
            q10,
            "q10_evolucion_anual",
        )
    )

    # =========================================================
    # TIEMPOS
    # =========================================================

    separator(
        "RESUMEN DE TIEMPOS - SPARK"
    )

    timings_df = spark.createDataFrame(
        timings
    )

    timings_df.orderBy(
        "query_id"
    ).show(
        20,
        truncate=False,
    )

    total_time = sum(
        row[
            "tiempo_segundos"
        ]
        for row in timings
    )

    print(
        f"\nTiempo total consultas: "
        f"{total_time:.4f} segundos"
    )

    (
        timings_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(
            f"{OUTPUT_BASE}/"
            "tiempos_spark"
        )
    )

    separator(
        "[OK] SPARK FINALIZADO"
    )

    spark.stop()


if __name__ == "__main__":
    main()

