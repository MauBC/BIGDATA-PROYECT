from pathlib import Path
import sys
import time

import polars as pl


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DATASET_PATH = Path("data/processed/videogames_clean.parquet")

EXPECTED_ROWS = 899_585
EXPECTED_COLUMNS = 60


def print_check(name: str, condition: bool, detail: str = "") -> bool:
    status = "OK" if condition else "ERROR"

    if detail:
        print(f"[{status}] {name}: {detail}")
    else:
        print(f"[{status}] {name}")

    return condition


def main() -> None:
    print("=" * 100)
    print("VALIDACION DEL DATASET PROCESADO - RAWG")
    print("=" * 100)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro: {DATASET_PATH.resolve()}"
        )

    start = time.perf_counter()

    df = pl.scan_parquet(DATASET_PATH)
    schema = df.collect_schema()

    checks = []

    # ------------------------------------------------------------------
    # 1. DIMENSIONES
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("1. DIMENSIONES")
    print("=" * 100)

    row_count = (
        df.select(pl.len().alias("rows"))
        .collect()
        .item()
    )

    column_count = len(schema)

    checks.append(
        print_check(
            "Cantidad de filas",
            row_count == EXPECTED_ROWS,
            f"{row_count:,}"
        )
    )

    checks.append(
        print_check(
            "Cantidad de columnas",
            column_count == EXPECTED_COLUMNS,
            str(column_count)
        )
    )

    # ------------------------------------------------------------------
    # 2. UNICIDAD E IDENTIFICADORES
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("2. UNICIDAD E IDENTIFICADORES")
    print("=" * 100)

    duplicate_ids = (
        df.group_by("id")
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len())
        .collect()
        .item()
    )

    duplicate_slugs = (
        df
        .filter(pl.col("slug").is_not_null())
        .group_by("slug")
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len())
        .collect()
        .item()
    )

    null_ids = (
        df
        .filter(pl.col("id").is_null())
        .select(pl.len())
        .collect()
        .item()
    )

    null_names = (
        df
        .filter(pl.col("name").is_null())
        .select(pl.len())
        .collect()
        .item()
    )

    null_slugs = (
        df
        .filter(pl.col("slug").is_null())
        .select(pl.len())
        .collect()
        .item()
    )

    checks.append(
        print_check(
            "IDs duplicados",
            duplicate_ids == 0,
            f"{duplicate_ids:,}"
        )
    )

    checks.append(
        print_check(
            "Slugs duplicados",
            duplicate_slugs == 0,
            f"{duplicate_slugs:,}"
        )
    )

    checks.append(
        print_check(
            "IDs NULL",
            null_ids == 0,
            f"{null_ids:,}"
        )
    )

    checks.append(
        print_check(
            "Names NULL",
            null_names == 0,
            f"{null_names:,}"
        )
    )

    checks.append(
        print_check(
            "Slugs NULL",
            null_slugs == 0,
            f"{null_slugs:,}"
        )
    )

    # ------------------------------------------------------------------
    # 3. CASOS LITERALES ESPECIALES
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("3. CASOS LITERALES ESPECIALES")
    print("=" * 100)

    expected_special_values = {
        100122: ("NULL", "null"),
        468408: ("None", "none"),
        119609: ("NaN", "nan"),
    }

    special_rows = (
        df
        .filter(
            pl.col("id").is_in(
                list(expected_special_values.keys())
            )
        )
        .select(
            "id",
            "name",
            "slug",
        )
        .collect()
    )

    special_lookup = {
        row["id"]: (
            row["name"],
            row["slug"],
        )
        for row in special_rows.iter_rows(named=True)
    }

    for game_id, expected_values in expected_special_values.items():
        actual_values = special_lookup.get(game_id)

        checks.append(
            print_check(
                f"Literal ID {game_id}",
                actual_values == expected_values,
                f"name={actual_values[0]!r}, slug={actual_values[1]!r}"
                if actual_values
                else "registro no encontrado"
            )
        )

    # ------------------------------------------------------------------
    # 4. TIPOS DE DATOS
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("4. TIPOS PRINCIPALES")
    print("=" * 100)

    expected_types = {
        "id": pl.Int64,
        "released": pl.Date,
        "release_year": pl.Int32,
        "rating": pl.Float64,
        "rating_top": pl.Int64,
        "ratings_count": pl.Int64,
        "metacritic": pl.Int64,
        "playtime": pl.Float64,
        "reviews_count": pl.Int64,
        "genres_count": pl.Int32,
        "platforms_count": pl.Int32,
        "developers_count_clean": pl.Int32,
        "publishers_count": pl.Int32,
    }

    for column, expected_type in expected_types.items():
        actual_type = schema[column]

        checks.append(
            print_check(
                column,
                actual_type == expected_type,
                f"{actual_type}"
            )
        )

    # ------------------------------------------------------------------
    # 5. COHERENCIA RELEASED / RELEASE_YEAR
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("5. COHERENCIA DE FECHAS")
    print("=" * 100)

    invalid_release_year = (
        df.filter(
            (
                pl.col("released").is_not_null()
                & (
                    pl.col("release_year")
                    != pl.col("released").dt.year()
                )
            )
            |
            (
                pl.col("released").is_null()
                & pl.col("release_year").is_not_null()
            )
        )
        .select(pl.len())
        .collect()
        .item()
    )

    checks.append(
        print_check(
            "released vs release_year",
            invalid_release_year == 0,
            f"{invalid_release_year:,} inconsistencias"
        )
    )

    date_range = (
        df.select(
            pl.col("released").min().alias("fecha_minima"),
            pl.col("released").max().alias("fecha_maxima"),
        )
        .collect()
    )

    print(date_range)

    # ------------------------------------------------------------------
    # 6. VARIABLES DERIVADAS
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("6. VALIDACION DE VARIABLES DERIVADAS")
    print("=" * 100)

    derived_rules = {
        "genres": "genres_count",
        "platforms": "platforms_count",
        "developers": "developers_count_clean",
        "publishers": "publishers_count",
    }

    for source_column, count_column in derived_rules.items():

        expected_count = (
            pl.when(pl.col(source_column).is_null())
            .then(0)
            .otherwise(
                pl.col(source_column)
                .str.count_matches(r"\|") + 1
            )
            .cast(pl.Int32)
        )

        invalid_count = (
            df.filter(
                pl.col(count_column) != expected_count
            )
            .select(pl.len())
            .collect()
            .item()
        )

        checks.append(
            print_check(
                count_column,
                invalid_count == 0,
                f"{invalid_count:,} inconsistencias"
            )
        )

    # ------------------------------------------------------------------
    # 7. RANGOS NUMERICOS
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("7. RANGOS NUMERICOS")
    print("=" * 100)

    numeric_summary = (
        df.select(
            pl.col("rating").min().alias("rating_min"),
            pl.col("rating").max().alias("rating_max"),
            pl.col("metacritic").min().alias("metacritic_min"),
            pl.col("metacritic").max().alias("metacritic_max"),
            pl.col("playtime").min().alias("playtime_min"),
            pl.col("playtime").max().alias("playtime_max"),
        )
        .collect()
    )

    print(numeric_summary)

    # ------------------------------------------------------------------
    # 8. NULLS PRINCIPALES
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("8. NULLS PRINCIPALES DESPUES DE LIMPIEZA")
    print("=" * 100)

    null_columns = [
        "released",
        "rating",
        "ratings_count",
        "metacritic",
        "playtime",
        "genres",
        "platforms",
        "developers",
        "publishers",
    ]

    null_result = (
        df.select(
            [
                pl.col(column)
                .null_count()
                .alias(column)
                for column in null_columns
            ]
        )
        .collect()
        .row(0, named=True)
    )

    for column, count in null_result.items():
        percentage = count / row_count * 100

        print(
            f"{column:<25}"
            f"{count:>12,}"
            f" ({percentage:>6.2f}%)"
        )

    # ------------------------------------------------------------------
    # RESULTADO FINAL
    # ------------------------------------------------------------------

    elapsed = time.perf_counter() - start

    print("\n" + "=" * 100)

    if all(checks):
        print("[OK] DATASET PROCESADO VALIDADO CORRECTAMENTE")
    else:
        print("[ERROR] SE ENCONTRARON VALIDACIONES FALLIDAS")

    print(f"Tiempo de validacion: {elapsed:.2f} segundos")
    print("=" * 100)

    if not all(checks):
        sys.exit(1)


if __name__ == "__main__":
    main()
