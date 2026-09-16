from pathlib import Path
import sys
import time

import polars as pl


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DATASET_PATH = Path("data/processed/videogames_clean.parquet")
OUTPUT_DIR = Path("results/polars")
TIMINGS_PATH = OUTPUT_DIR / "tiempos_polars.csv"


def separator(title: str) -> None:
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def run_query(
    query_id: str,
    title: str,
    query: pl.LazyFrame,
    output_name: str,
) -> dict:
    separator(f"{query_id} - {title}")

    start = time.perf_counter()

    result = query.collect()

    elapsed = time.perf_counter() - start

    output_path = OUTPUT_DIR / output_name
    result.write_csv(output_path)

    print(result)
    print(f"\nFilas resultado : {result.height:,}")
    print(f"Tiempo          : {elapsed:.4f} segundos")
    print(f"Archivo         : {output_path}")

    return {
        "query_id": query_id,
        "consulta": title,
        "filas_resultado": result.height,
        "tiempo_segundos": round(elapsed, 6),
        "archivo": str(output_path),
    }


def explode_dimension(
    df: pl.LazyFrame,
    column: str,
    extra_columns: list[str] | None = None,
) -> pl.LazyFrame:
    columns = ["id", column]

    if extra_columns:
        columns.extend(extra_columns)

    return (
        df
        .filter(pl.col(column).is_not_null())
        .select(columns)
        .with_columns(
            pl.col(column)
            .str.split("|")
        )
        .explode(column)
        .with_columns(
            pl.col(column)
            .str.strip_chars()
        )
        .filter(
            pl.col(column).is_not_null()
            & (pl.col(column) != "")
        )
    )


def main() -> None:
    separator("POLARS - 10 CONSULTAS OFICIALES RAWG")

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro: {DATASET_PATH.resolve()}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pl.scan_parquet(DATASET_PATH)

    timings = []

    # ================================================================
    # Q01
    # ================================================================

    q01 = df.select(
        pl.len().alias("total_registros"),
        pl.col("id").n_unique().alias("ids_unicos"),
        pl.col("release_year").min().alias("anio_minimo"),
        pl.col("release_year").max().alias("anio_maximo"),
        pl.col("rating").is_not_null().sum().alias("juegos_con_rating"),
        pl.col("metacritic").is_not_null().sum().alias("juegos_con_metacritic"),
        pl.col("genres").is_not_null().sum().alias("juegos_con_genero"),
        pl.col("platforms").is_not_null().sum().alias("juegos_con_plataforma"),
    )

    timings.append(
        run_query(
            "Q01",
            "Resumen general y cobertura",
            q01,
            "q01_resumen_general.csv",
        )
    )

    # ================================================================
    # Q02
    # ================================================================

    q02 = (
        df
        .filter(pl.col("release_year").is_not_null())
        .group_by("release_year")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("videojuegos")
        )
        .sort("release_year")
    )

    timings.append(
        run_query(
            "Q02",
            "Videojuegos lanzados por año",
            q02,
            "q02_videojuegos_por_anio.csv",
        )
    )

    # ================================================================
    # Q03
    # ================================================================

    genres = explode_dimension(
        df,
        "genres",
    )

    q03 = (
        genres
        .group_by("genres")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("videojuegos")
        )
        .sort(
            "videojuegos",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q03",
            "Top 20 generos por cantidad de videojuegos",
            q03,
            "q03_top_generos.csv",
        )
    )

    # ================================================================
    # Q04
    # ================================================================

    platforms = explode_dimension(
        df,
        "platforms",
    )

    q04 = (
        platforms
        .group_by("platforms")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("videojuegos")
        )
        .sort(
            "videojuegos",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q04",
            "Top 20 plataformas por cantidad de videojuegos",
            q04,
            "q04_top_plataformas.csv",
        )
    )

    # ================================================================
    # Q05
    # ================================================================

    developers = explode_dimension(
        df,
        "developers",
    )

    q05 = (
        developers
        .group_by("developers")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("videojuegos")
        )
        .sort(
            "videojuegos",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q05",
            "Top 20 desarrolladores por cantidad de videojuegos",
            q05,
            "q05_top_desarrolladores.csv",
        )
    )

    # ================================================================
    # Q06
    # ================================================================

    rating_genres = (
        df
        .filter(
            pl.col("genres").is_not_null()
            & pl.col("rating").is_not_null()
        )
        .select(
            "id",
            "genres",
            "rating",
        )
        .with_columns(
            pl.col("genres").str.split("|")
        )
        .explode("genres")
        .with_columns(
            pl.col("genres").str.strip_chars()
        )
        .filter(pl.col("genres") != "")
    )

    q06 = (
        rating_genres
        .group_by("genres")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("juegos_con_rating"),

            pl.col("rating")
            .mean()
            .round(4)
            .alias("rating_promedio"),

            pl.col("rating")
            .median()
            .round(4)
            .alias("rating_mediano"),
        )
        .filter(
            pl.col("juegos_con_rating") >= 50
        )
        .sort(
            "rating_promedio",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q06",
            "Rating promedio por genero",
            q06,
            "q06_rating_por_genero.csv",
        )
    )

    # ================================================================
    # Q07
    # ================================================================

    metacritic_genres = (
        df
        .filter(
            pl.col("genres").is_not_null()
            & pl.col("metacritic").is_not_null()
        )
        .select(
            "id",
            "genres",
            "metacritic",
        )
        .with_columns(
            pl.col("genres").str.split("|")
        )
        .explode("genres")
        .with_columns(
            pl.col("genres").str.strip_chars()
        )
        .filter(pl.col("genres") != "")
    )

    q07 = (
        metacritic_genres
        .group_by("genres")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("juegos_con_metacritic"),

            pl.col("metacritic")
            .mean()
            .round(4)
            .alias("metacritic_promedio"),

            pl.col("metacritic")
            .median()
            .alias("metacritic_mediano"),
        )
        .filter(
            pl.col("juegos_con_metacritic") >= 20
        )
        .sort(
            "metacritic_promedio",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q07",
            "Metacritic promedio por genero",
            q07,
            "q07_metacritic_por_genero.csv",
        )
    )

    # ================================================================
    # Q08
    # ================================================================

    q08 = (
        df
        .filter(
            pl.col("ratings_count").is_not_null()
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
        .sort(
            "ratings_count",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q08",
            "Top 20 videojuegos por cantidad de ratings",
            q08,
            "q08_top_videojuegos_ratings.csv",
        )
    )

    # ================================================================
    # Q09
    # ================================================================

    playtime_genres = (
        df
        .filter(
            pl.col("genres").is_not_null()
            & pl.col("playtime").is_not_null()
        )
        .select(
            "id",
            "genres",
            "playtime",
        )
        .with_columns(
            pl.col("genres").str.split("|")
        )
        .explode("genres")
        .with_columns(
            pl.col("genres").str.strip_chars()
        )
        .filter(pl.col("genres") != "")
    )

    q09 = (
        playtime_genres
        .group_by("genres")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("juegos_con_playtime"),

            pl.col("playtime")
            .mean()
            .round(4)
            .alias("playtime_promedio"),

            pl.col("playtime")
            .median()
            .round(4)
            .alias("playtime_mediano"),

            pl.col("playtime")
            .max()
            .alias("playtime_maximo"),
        )
        .filter(
            pl.col("juegos_con_playtime") >= 30
        )
        .sort(
            "playtime_promedio",
            descending=True,
        )
        .head(20)
    )

    timings.append(
        run_query(
            "Q09",
            "Playtime promedio por genero",
            q09,
            "q09_playtime_por_genero.csv",
        )
    )

    # ================================================================
    # Q10
    # ================================================================

    q10 = (
        df
        .filter(
            pl.col("release_year").is_not_null()
        )
        .group_by("release_year")
        .agg(
            pl.col("id")
            .n_unique()
            .alias("videojuegos"),

            pl.col("rating")
            .is_not_null()
            .sum()
            .alias("juegos_con_rating"),

            pl.col("rating")
            .mean()
            .round(4)
            .alias("rating_promedio"),

            pl.col("metacritic")
            .is_not_null()
            .sum()
            .alias("juegos_con_metacritic"),

            pl.col("metacritic")
            .mean()
            .round(4)
            .alias("metacritic_promedio"),

            pl.col("ratings_count")
            .sum()
            .alias("total_ratings"),
        )
        .sort("release_year")
    )

    timings.append(
        run_query(
            "Q10",
            "Evolucion anual de videojuegos y valoraciones",
            q10,
            "q10_evolucion_anual.csv",
        )
    )

    # ================================================================
    # TIEMPOS
    # ================================================================

    separator("RESUMEN DE TIEMPOS - POLARS")

    timings_df = pl.DataFrame(timings)

    timings_df.write_csv(TIMINGS_PATH)

    print(timings_df)

    total_time = timings_df[
        "tiempo_segundos"
    ].sum()

    print(
        f"\nTiempo total consultas: "
        f"{total_time:.4f} segundos"
    )

    print(
        f"Resumen guardado en: {TIMINGS_PATH}"
    )

    separator("[OK] POLARS FINALIZADO")


if __name__ == "__main__":
    main()
