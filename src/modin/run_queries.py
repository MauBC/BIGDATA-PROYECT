from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# IMPORTANTE: seleccionar Ray antes de importar modin.pandas.
os.environ["MODIN_ENGINE"] = "ray"

import modin
import modin.pandas as pd
import pandas
import pyarrow
import ray
from modin.utils import execute


DATASET = Path("data/processed/videogames_clean.parquet")
OUTPUT_DIR = Path("results/modin")


def separator(title: str) -> None:
    print()
    print("=" * 110)
    print(title)
    print("=" * 110)


def explode_dimension(
    df,
    column: str,
    extra_columns: list[str] | None = None,
):
    columns = ["id", column]

    if extra_columns:
        columns.extend(extra_columns)

    work = df[columns].dropna(subset=[column]).copy()

    work[column] = work[column].str.split(
        "|",
        regex=False,
    )

    work = work.explode(column)

    work[column] = work[column].str.strip()

    work = work[
        work[column].notna()
        & (work[column] != "")
    ]

    return work


def save_result(
    query_id: str,
    title: str,
    filename: str,
    result,
    start_time: float,
) -> dict:

    # Fuerza a que Modin/Ray termine la computacion pendiente.
    execute(result)

    elapsed = time.perf_counter() - start_time

    output_path = OUTPUT_DIR / filename

    result.to_csv(
        output_path,
        index=False,
    )

    separator(f"{query_id} - {title}")

    print(result)
    print()
    print(f"Filas resultado : {len(result)}")
    print(f"Tiempo          : {elapsed:.4f} segundos")
    print(f"Archivo         : {output_path}")

    return {
        "query_id": query_id,
        "consulta": title,
        "filas_resultado": len(result),
        "tiempo_segundos": elapsed,
        "archivo": str(output_path),
    }


def main() -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    separator("MODIN + RAY - 10 CONSULTAS OFICIALES RAWG")

    print(f"Python  : {sys.version.split()[0]}")
    print(f"Modin   : {modin.__version__}")
    print(f"Ray     : {ray.__version__}")
    print(f"pandas  : {pandas.__version__}")
    print(f"PyArrow : {pyarrow.__version__}")
    print(f"Engine  : {os.environ.get('MODIN_ENGINE')}")
    print(f"Dataset : {DATASET}")

    print()
    print("Cargando dataset...")

    load_start = time.perf_counter()

    df = pd.read_parquet(DATASET)

    execute(df)

    load_time = time.perf_counter() - load_start

    print(f"Filas    : {len(df):,}")
    print(f"Columnas : {len(df.columns)}")
    print(f"Carga    : {load_time:.4f} segundos")

    timings = []

    # =========================================================
    # Q01
    # =========================================================

    start = time.perf_counter()

    q01 = pd.DataFrame(
        [
            {
                "total_registros": len(df),
                "ids_unicos": int(df["id"].nunique()),
                "anio_minimo": int(df["release_year"].min()),
                "anio_maximo": int(df["release_year"].max()),
                "juegos_con_rating": int(df["rating"].count()),
                "juegos_con_metacritic": int(
                    df["metacritic"].count()
                ),
                "juegos_con_genero": int(df["genres"].count()),
                "juegos_con_plataforma": int(
                    df["platforms"].count()
                ),
            }
        ]
    )

    timings.append(
        save_result(
            "Q01",
            "Resumen general y cobertura",
            "q01_resumen_general.csv",
            q01,
            start,
        )
    )

    # =========================================================
    # Q02
    # =========================================================

    start = time.perf_counter()

    q02 = (
        df[
            df["release_year"].notna()
        ][["release_year", "id"]]
        .groupby("release_year")["id"]
        .nunique()
        .reset_index()
        .rename(
            columns={
                "id": "videojuegos"
            }
        )
        .sort_values(
            "release_year",
            ascending=True,
        )
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q02",
            "Videojuegos lanzados por año",
            "q02_videojuegos_por_anio.csv",
            q02,
            start,
        )
    )

    # =========================================================
    # Q03
    # =========================================================

    start = time.perf_counter()

    genres = explode_dimension(
        df,
        "genres",
    )

    q03 = (
        genres
        .groupby("genres")["id"]
        .nunique()
        .reset_index()
        .rename(
            columns={
                "id": "videojuegos"
            }
        )
        .sort_values(
            ["videojuegos", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q03",
            "Top 20 generos por cantidad de videojuegos",
            "q03_top_generos.csv",
            q03,
            start,
        )
    )

    # =========================================================
    # Q04
    # =========================================================

    start = time.perf_counter()

    platforms = explode_dimension(
        df,
        "platforms",
    )

    q04 = (
        platforms
        .groupby("platforms")["id"]
        .nunique()
        .reset_index()
        .rename(
            columns={
                "id": "videojuegos"
            }
        )
        .sort_values(
            ["videojuegos", "platforms"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q04",
            "Top 20 plataformas por cantidad de videojuegos",
            "q04_top_plataformas.csv",
            q04,
            start,
        )
    )

    # =========================================================
    # Q05
    # =========================================================

    start = time.perf_counter()

    developers = explode_dimension(
        df,
        "developers",
    )

    q05 = (
        developers
        .groupby("developers")["id"]
        .nunique()
        .reset_index()
        .rename(
            columns={
                "id": "videojuegos"
            }
        )
        .sort_values(
            ["videojuegos", "developers"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q05",
            "Top 20 desarrolladores por cantidad de videojuegos",
            "q05_top_desarrolladores.csv",
            q05,
            start,
        )
    )

    # =========================================================
    # Q06
    # =========================================================

    start = time.perf_counter()

    rating_genres = explode_dimension(
        df,
        "genres",
        extra_columns=["rating"],
    )

    rating_genres = rating_genres[
        rating_genres["rating"].notna()
    ]

    rating_count = (
        rating_genres
        .groupby("genres")["id"]
        .nunique()
        .rename("juegos_con_rating")
    )

    rating_mean = (
        rating_genres
        .groupby("genres")["rating"]
        .mean()
        .rename("rating_promedio")
    )

    rating_median = (
        rating_genres
        .groupby("genres")["rating"]
        .median()
        .rename("rating_mediano")
    )

    q06 = pd.concat(
        [
            rating_count,
            rating_mean,
            rating_median,
        ],
        axis=1,
    ).reset_index()

    q06["rating_promedio"] = (
        q06["rating_promedio"].round(4)
    )

    q06["rating_mediano"] = (
        q06["rating_mediano"].round(4)
    )

    q06 = (
        q06[
            q06["juegos_con_rating"] >= 50
        ]
        .sort_values(
            ["rating_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q06",
            "Rating promedio por genero",
            "q06_rating_por_genero.csv",
            q06,
            start,
        )
    )

    # =========================================================
    # Q07
    # =========================================================

    start = time.perf_counter()

    metacritic_genres = explode_dimension(
        df,
        "genres",
        extra_columns=["metacritic"],
    )

    metacritic_genres = metacritic_genres[
        metacritic_genres["metacritic"].notna()
    ]

    meta_count = (
        metacritic_genres
        .groupby("genres")["id"]
        .nunique()
        .rename("juegos_con_metacritic")
    )

    meta_mean = (
        metacritic_genres
        .groupby("genres")["metacritic"]
        .mean()
        .rename("metacritic_promedio")
    )

    meta_median = (
        metacritic_genres
        .groupby("genres")["metacritic"]
        .median()
        .rename("metacritic_mediano")
    )

    q07 = pd.concat(
        [
            meta_count,
            meta_mean,
            meta_median,
        ],
        axis=1,
    ).reset_index()

    q07["metacritic_promedio"] = (
        q07["metacritic_promedio"].round(4)
    )

    q07["metacritic_mediano"] = (
        q07["metacritic_mediano"].round(4)
    )

    q07 = (
        q07[
            q07["juegos_con_metacritic"] >= 20
        ]
        .sort_values(
            ["metacritic_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q07",
            "Metacritic promedio por genero",
            "q07_metacritic_por_genero.csv",
            q07,
            start,
        )
    )

    # =========================================================
    # Q08
    # =========================================================

    start = time.perf_counter()

    q08 = (
        df[
            df["ratings_count"].notna()
        ][
            [
                "id",
                "name",
                "release_year",
                "rating",
                "ratings_count",
                "reviews_count",
                "genres",
            ]
        ]
        .sort_values(
            "ratings_count",
            ascending=False,
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q08",
            "Top 20 videojuegos por cantidad de ratings",
            "q08_top_videojuegos_ratings.csv",
            q08,
            start,
        )
    )

    # =========================================================
    # Q09
    # =========================================================

    start = time.perf_counter()

    playtime_genres = explode_dimension(
        df,
        "genres",
        extra_columns=["playtime"],
    )

    playtime_genres = playtime_genres[
        playtime_genres["playtime"].notna()
    ]

    play_count = (
        playtime_genres
        .groupby("genres")["id"]
        .nunique()
        .rename("juegos_con_playtime")
    )

    play_mean = (
        playtime_genres
        .groupby("genres")["playtime"]
        .mean()
        .rename("playtime_promedio")
    )

    play_median = (
        playtime_genres
        .groupby("genres")["playtime"]
        .median()
        .rename("playtime_mediano")
    )

    play_max = (
        playtime_genres
        .groupby("genres")["playtime"]
        .max()
        .rename("playtime_maximo")
    )

    q09 = pd.concat(
        [
            play_count,
            play_mean,
            play_median,
            play_max,
        ],
        axis=1,
    ).reset_index()

    q09["playtime_promedio"] = (
        q09["playtime_promedio"].round(4)
    )

    q09["playtime_mediano"] = (
        q09["playtime_mediano"].round(4)
    )

    q09 = (
        q09[
            q09["juegos_con_playtime"] >= 30
        ]
        .sort_values(
            ["playtime_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q09",
            "Playtime promedio por genero",
            "q09_playtime_por_genero.csv",
            q09,
            start,
        )
    )

    # =========================================================
    # Q10
    # =========================================================

    start = time.perf_counter()

    yearly = df[
        df["release_year"].notna()
    ]

    year_games = (
        yearly
        .groupby("release_year")["id"]
        .nunique()
        .rename("videojuegos")
    )

    year_rating_count = (
        yearly
        .groupby("release_year")["rating"]
        .count()
        .rename("juegos_con_rating")
    )

    year_rating_mean = (
        yearly
        .groupby("release_year")["rating"]
        .mean()
        .rename("rating_promedio")
    )

    year_meta_count = (
        yearly
        .groupby("release_year")["metacritic"]
        .count()
        .rename("juegos_con_metacritic")
    )

    year_meta_mean = (
        yearly
        .groupby("release_year")["metacritic"]
        .mean()
        .rename("metacritic_promedio")
    )

    year_total_ratings = (
        yearly
        .groupby("release_year")["ratings_count"]
        .sum()
        .rename("total_ratings")
    )

    q10 = pd.concat(
        [
            year_games,
            year_rating_count,
            year_rating_mean,
            year_meta_count,
            year_meta_mean,
            year_total_ratings,
        ],
        axis=1,
    ).reset_index()

    q10["rating_promedio"] = (
        q10["rating_promedio"].round(4)
    )

    q10["metacritic_promedio"] = (
        q10["metacritic_promedio"].round(4)
    )

    q10 = (
        q10
        .sort_values(
            "release_year",
            ascending=True,
        )
        .reset_index(drop=True)
    )

    timings.append(
        save_result(
            "Q10",
            "Evolucion anual de videojuegos y valoraciones",
            "q10_evolucion_anual.csv",
            q10,
            start,
        )
    )

    # =========================================================
    # TIEMPOS
    # =========================================================

    separator("RESUMEN DE TIEMPOS - MODIN + RAY")

    timings_df = pandas.DataFrame(timings)

    print(
        timings_df[
            [
                "query_id",
                "filas_resultado",
                "tiempo_segundos",
            ]
        ].to_string(
            index=False
        )
    )

    total_queries = timings_df[
        "tiempo_segundos"
    ].sum()

    print()
    print(
        f"Tiempo carga dataset    : "
        f"{load_time:.4f} segundos"
    )

    print(
        f"Tiempo total consultas  : "
        f"{total_queries:.4f} segundos"
    )

    print(
        f"Tiempo carga + consultas: "
        f"{load_time + total_queries:.4f} segundos"
    )

    timings_df.to_csv(
        OUTPUT_DIR / "tiempos_modin.csv",
        index=False,
    )

    separator("[OK] MODIN FINALIZADO")

    if ray.is_initialized():
        ray.shutdown()


if __name__ == "__main__":
    main()
