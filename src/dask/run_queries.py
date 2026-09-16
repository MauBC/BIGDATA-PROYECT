from pathlib import Path
import sys
import time

import dask
import dask.dataframe as dd
import pandas as pd

from dask.distributed import Client, LocalCluster


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DATASET_PATH = Path("data/processed/videogames_clean.parquet")
OUTPUT_DIR = Path("results/dask")
TIMINGS_PATH = OUTPUT_DIR / "tiempos_dask.csv"


def separator(title: str) -> None:
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def save_result(
    query_id: str,
    title: str,
    result: pd.DataFrame,
    elapsed: float,
    output_name: str,
) -> dict:
    output_path = OUTPUT_DIR / output_name

    result.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    separator(f"{query_id} - {title}")

    print(result.to_string(index=False))

    print(f"\nFilas resultado : {len(result):,}")
    print(f"Tiempo          : {elapsed:.4f} segundos")
    print(f"Archivo         : {output_path}")

    return {
        "query_id": query_id,
        "consulta": title,
        "filas_resultado": len(result),
        "tiempo_segundos": round(elapsed, 6),
        "archivo": str(output_path),
    }


def explode_dimension(
    df: dd.DataFrame,
    column: str,
    extra_columns: list[str] | None = None,
) -> dd.DataFrame:

    columns = ["id", column]

    if extra_columns:
        columns.extend(extra_columns)

    work = df[columns]

    work = work.dropna(
        subset=[column]
    )

    work = work.assign(
        **{
            column: work[column].str.split("|")
        }
    )

    work = work.explode(column)

    work = work.assign(
        **{
            column: work[column].str.strip()
        }
    )

    work = work[
        work[column].notnull()
        & (work[column] != "")
    ]

    return work


def q01(df: dd.DataFrame) -> pd.DataFrame:
    start = time.perf_counter()

    (
        total_registros,
        ids_unicos,
        anio_minimo,
        anio_maximo,
        juegos_con_rating,
        juegos_con_metacritic,
        juegos_con_genero,
        juegos_con_plataforma,
    ) = dask.compute(
        df.map_partitions(len).sum(),
        df["id"].nunique(),
        df["release_year"].min(),
        df["release_year"].max(),
        df["rating"].count(),
        df["metacritic"].count(),
        df["genres"].count(),
        df["platforms"].count(),
    )

    result = pd.DataFrame(
        [
            {
                "total_registros": int(total_registros),
                "ids_unicos": int(ids_unicos),
                "anio_minimo": int(anio_minimo),
                "anio_maximo": int(anio_maximo),
                "juegos_con_rating": int(juegos_con_rating),
                "juegos_con_metacritic": int(juegos_con_metacritic),
                "juegos_con_genero": int(juegos_con_genero),
                "juegos_con_plataforma": int(juegos_con_plataforma),
            }
        ]
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q02(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    work = df.dropna(
        subset=["release_year"]
    )

    result = (
        work
        .groupby("release_year")["id"]
        .nunique()
        .compute()
        .rename("videojuegos")
        .reset_index()
        .sort_values("release_year")
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q03(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    genres = explode_dimension(
        df,
        "genres",
    )

    result = (
        genres
        .groupby("genres")["id"]
        .nunique()
        .compute()
        .rename("videojuegos")
        .reset_index()
        .sort_values(
            ["videojuegos", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q04(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    platforms = explode_dimension(
        df,
        "platforms",
    )

    result = (
        platforms
        .groupby("platforms")["id"]
        .nunique()
        .compute()
        .rename("videojuegos")
        .reset_index()
        .sort_values(
            ["videojuegos", "platforms"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q05(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    developers = explode_dimension(
        df,
        "developers",
    )

    result = (
        developers
        .groupby("developers")["id"]
        .nunique()
        .compute()
        .rename("videojuegos")
        .reset_index()
        .sort_values(
            ["videojuegos", "developers"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q06(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    genres = explode_dimension(
        df,
        "genres",
        extra_columns=["rating"],
    )

    genres = genres.dropna(
        subset=["rating"]
    )

    grouped = genres.groupby("genres")

    count_task = grouped["id"].nunique()
    mean_task = grouped["rating"].mean()
    median_task = grouped["rating"].median()

    counts, means, medians = dask.compute(
        count_task,
        mean_task,
        median_task,
    )

    result = pd.concat(
        [
            counts.rename("juegos_con_rating"),
            means.rename("rating_promedio"),
            medians.rename("rating_mediano"),
        ],
        axis=1,
    ).reset_index()

    result["rating_promedio"] = (
        result["rating_promedio"].round(4)
    )

    result["rating_mediano"] = (
        result["rating_mediano"].round(4)
    )

    result = (
        result[
            result["juegos_con_rating"] >= 50
        ]
        .sort_values(
            ["rating_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q07(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    genres = explode_dimension(
        df,
        "genres",
        extra_columns=["metacritic"],
    )

    genres = genres.dropna(
        subset=["metacritic"]
    )

    grouped = genres.groupby("genres")

    count_task = grouped["id"].nunique()
    mean_task = grouped["metacritic"].mean()
    median_task = grouped["metacritic"].median()

    counts, means, medians = dask.compute(
        count_task,
        mean_task,
        median_task,
    )

    result = pd.concat(
        [
            counts.rename("juegos_con_metacritic"),
            means.rename("metacritic_promedio"),
            medians.rename("metacritic_mediano"),
        ],
        axis=1,
    ).reset_index()

    result["metacritic_promedio"] = (
        result["metacritic_promedio"].round(4)
    )

    result["metacritic_mediano"] = (
        result["metacritic_mediano"].round(4)
    )

    result = (
        result[
            result["juegos_con_metacritic"] >= 20
        ]
        .sort_values(
            ["metacritic_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q08(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    columns = [
        "id",
        "name",
        "release_year",
        "rating",
        "ratings_count",
        "reviews_count",
        "genres",
    ]

    work = (
        df[columns]
        .dropna(
            subset=["ratings_count"]
        )
    )

    result = (
        work
        .nlargest(
            20,
            "ratings_count",
        )
        .compute()
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q09(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    genres = explode_dimension(
        df,
        "genres",
        extra_columns=["playtime"],
    )

    genres = genres.dropna(
        subset=["playtime"]
    )

    grouped = genres.groupby("genres")

    count_task = grouped["id"].nunique()
    mean_task = grouped["playtime"].mean()
    median_task = grouped["playtime"].median()
    max_task = grouped["playtime"].max()

    (
        counts,
        means,
        medians,
        maximums,
    ) = dask.compute(
        count_task,
        mean_task,
        median_task,
        max_task,
    )

    result = pd.concat(
        [
            counts.rename("juegos_con_playtime"),
            means.rename("playtime_promedio"),
            medians.rename("playtime_mediano"),
            maximums.rename("playtime_maximo"),
        ],
        axis=1,
    ).reset_index()

    result["playtime_promedio"] = (
        result["playtime_promedio"].round(4)
    )

    result["playtime_mediano"] = (
        result["playtime_mediano"].round(4)
    )

    result = (
        result[
            result["juegos_con_playtime"] >= 30
        ]
        .sort_values(
            ["playtime_promedio", "genres"],
            ascending=[False, True],
        )
        .head(20)
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def q10(df: dd.DataFrame) -> tuple[pd.DataFrame, float]:
    start = time.perf_counter()

    work = df.dropna(
        subset=["release_year"]
    )

    grouped = work.groupby(
        "release_year"
    )

    games_task = grouped["id"].nunique()
    rating_count_task = grouped["rating"].count()
    rating_mean_task = grouped["rating"].mean()
    metacritic_count_task = grouped["metacritic"].count()
    metacritic_mean_task = grouped["metacritic"].mean()
    ratings_total_task = grouped["ratings_count"].sum()

    (
        games,
        rating_counts,
        rating_means,
        metacritic_counts,
        metacritic_means,
        ratings_totals,
    ) = dask.compute(
        games_task,
        rating_count_task,
        rating_mean_task,
        metacritic_count_task,
        metacritic_mean_task,
        ratings_total_task,
    )

    result = pd.concat(
        [
            games.rename("videojuegos"),
            rating_counts.rename("juegos_con_rating"),
            rating_means.rename("rating_promedio"),
            metacritic_counts.rename(
                "juegos_con_metacritic"
            ),
            metacritic_means.rename(
                "metacritic_promedio"
            ),
            ratings_totals.rename("total_ratings"),
        ],
        axis=1,
    ).reset_index()

    result["rating_promedio"] = (
        result["rating_promedio"].round(4)
    )

    result["metacritic_promedio"] = (
        result["metacritic_promedio"].round(4)
    )

    result = (
        result
        .sort_values("release_year")
        .reset_index(drop=True)
    )

    elapsed = time.perf_counter() - start

    return result, elapsed


def main() -> None:
    separator("DASK - 10 CONSULTAS OFICIALES RAWG")

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro: {DATASET_PATH.resolve()}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("\nIniciando cluster Dask...")

    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        processes=True,
        memory_limit="2GB",
        dashboard_address=None,
    )

    client = Client(cluster)

    try:
        print(
            f"[OK] Workers activos: "
            f"{len(client.scheduler_info()['workers'])}"
        )

        print("\nLeyendo Parquet...")

        df = dd.read_parquet(
            DATASET_PATH,
            engine="pyarrow",
            split_row_groups="adaptive",
            blocksize="128MiB",
        )

        print(
            f"[OK] Particiones: {df.npartitions}"
        )

        print(
            f"[OK] Columnas: {len(df.columns)}"
        )

        timings = []

        # Q01
        result, elapsed = q01(df)
        timings.append(
            save_result(
                "Q01",
                "Resumen general y cobertura",
                result,
                elapsed,
                "q01_resumen_general.csv",
            )
        )

        # Q02
        result, elapsed = q02(df)
        timings.append(
            save_result(
                "Q02",
                "Videojuegos lanzados por año",
                result,
                elapsed,
                "q02_videojuegos_por_anio.csv",
            )
        )

        # Q03
        result, elapsed = q03(df)
        timings.append(
            save_result(
                "Q03",
                "Top 20 generos por cantidad de videojuegos",
                result,
                elapsed,
                "q03_top_generos.csv",
            )
        )

        # Q04
        result, elapsed = q04(df)
        timings.append(
            save_result(
                "Q04",
                "Top 20 plataformas por cantidad de videojuegos",
                result,
                elapsed,
                "q04_top_plataformas.csv",
            )
        )

        # Q05
        result, elapsed = q05(df)
        timings.append(
            save_result(
                "Q05",
                "Top 20 desarrolladores por cantidad de videojuegos",
                result,
                elapsed,
                "q05_top_desarrolladores.csv",
            )
        )

        # Q06
        result, elapsed = q06(df)
        timings.append(
            save_result(
                "Q06",
                "Rating promedio por genero",
                result,
                elapsed,
                "q06_rating_por_genero.csv",
            )
        )

        # Q07
        result, elapsed = q07(df)
        timings.append(
            save_result(
                "Q07",
                "Metacritic promedio por genero",
                result,
                elapsed,
                "q07_metacritic_por_genero.csv",
            )
        )

        # Q08
        result, elapsed = q08(df)
        timings.append(
            save_result(
                "Q08",
                "Top 20 videojuegos por cantidad de ratings",
                result,
                elapsed,
                "q08_top_videojuegos_ratings.csv",
            )
        )

        # Q09
        result, elapsed = q09(df)
        timings.append(
            save_result(
                "Q09",
                "Playtime promedio por genero",
                result,
                elapsed,
                "q09_playtime_por_genero.csv",
            )
        )

        # Q10
        result, elapsed = q10(df)
        timings.append(
            save_result(
                "Q10",
                "Evolucion anual de videojuegos y valoraciones",
                result,
                elapsed,
                "q10_evolucion_anual.csv",
            )
        )

        # ============================================================
        # RESUMEN DE TIEMPOS
        # ============================================================

        separator("RESUMEN DE TIEMPOS - DASK")

        timings_df = pd.DataFrame(timings)

        timings_df.to_csv(
            TIMINGS_PATH,
            index=False,
            encoding="utf-8",
        )

        print(
            timings_df.to_string(
                index=False
            )
        )

        total_time = (
            timings_df[
                "tiempo_segundos"
            ].sum()
        )

        print(
            f"\nTiempo total consultas: "
            f"{total_time:.4f} segundos"
        )

        print(
            f"Resumen guardado en: "
            f"{TIMINGS_PATH}"
        )

        separator("[OK] DASK FINALIZADO")

    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
