from pathlib import Path
import sys

import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

DATASET_PATH = Path("data/raw/videogames_data.csv")

MISSING_TOKENS = [
    "",
    "nan",
    "null",
    "none",
    "n/a",
    "na",
]

KEY_COLUMNS = [
    "released",
    "background_image",
    "rating",
    "rating_top",
    "ratings_count",
    "reviews_text_count",
    "added",
    "metacritic",
    "playtime",
    "suggestions_count",
    "reviews_count",
    "platforms",
    "developers",
    "genres",
    "publishers",
    "esrb_rating",
    "description",
    "description_raw",
    "tba",
]

NUMERIC_CANDIDATES = [
    "rating",
    "rating_top",
    "ratings_count",
    "reviews_text_count",
    "added",
    "metacritic",
    "playtime",
    "suggestions_count",
    "reviews_count",
    "movies_count",
    "reddit_count",
    "achievements_count",
    "youtube_count",
    "creators_count",
    "twitch_count",
    "parent_achievements_count",
    "additions_count",
    "screenshots_count",
    "game_series_count",
    "parents_count",
]


def as_clean_string(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
    )


def is_semantic_missing(column: str) -> pl.Expr:
    text = as_clean_string(column)

    return (
        pl.col(column).is_null()
        | text
        .str.to_lowercase()
        .is_in(MISSING_TOKENS)
        .fill_null(False)
    )


def main() -> None:
    print("=" * 90)
    print("PERFIL DE CALIDAD - RAWG VIDEOGAMES")
    print("=" * 90)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro: {DATASET_PATH.resolve()}"
        )

    df = pl.scan_csv(
        DATASET_PATH,
        infer_schema_length=10000,
    )

    schema = df.collect_schema()
    columns = set(schema.names())

    row_count = (
        df.select(pl.len().alias("rows"))
        .collect()
        .item()
    )

    print(f"\nRegistros totales: {row_count:,}")
    print(f"Columnas totales: {len(schema)}")

    # ------------------------------------------------------------
    # 1. DUPLICADOS POR ID
    # ------------------------------------------------------------

    print("\n" + "=" * 90)
    print("1. DUPLICADOS POR ID")
    print("=" * 90)

    duplicate_ids = (
        df.group_by("id")
        .len()
        .filter(pl.col("len") > 1)
        .select(
            pl.len().alias("ids_duplicados"),
            pl.col("len").sum().alias("filas_en_grupos_duplicados"),
            (pl.col("len") - 1).sum().alias("filas_extra"),
        )
        .collect()
    )

    print(duplicate_ids)

    # ------------------------------------------------------------
    # 2. DUPLICADOS POR SLUG
    # ------------------------------------------------------------

    print("\n" + "=" * 90)
    print("2. DUPLICADOS POR SLUG")
    print("=" * 90)

    duplicate_slugs = (
        df.group_by("slug")
        .len()
        .filter(pl.col("len") > 1)
        .select(
            pl.len().alias("slugs_duplicados"),
            pl.col("len").sum().alias("filas_en_grupos_duplicados"),
            (pl.col("len") - 1).sum().alias("filas_extra"),
        )
        .collect()
    )

    print(duplicate_slugs)

    # ------------------------------------------------------------
    # 3. VALORES FALTANTES REALES
    # ------------------------------------------------------------

    print("\n" + "=" * 90)
    print("3. VALORES FALTANTES REALES")
    print('Incluye NULL, "", "nan", "null", "none", "n/a" y "na"')
    print("=" * 90)

    available_key_columns = [
        column
        for column in KEY_COLUMNS
        if column in columns
    ]

    missing_result = (
        df.select(
            [
                is_semantic_missing(column)
                .sum()
                .alias(column)
                for column in available_key_columns
            ]
        )
        .collect()
        .row(0, named=True)
    )

    missing_sorted = sorted(
        missing_result.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    for column, count in missing_sorted:
        percentage = (
            count / row_count * 100
            if row_count
            else 0
        )

        print(
            f"{column:<30}"
            f"{count:>12,}"
            f"  ({percentage:>6.2f}%)"
        )

    # ------------------------------------------------------------
    # 4. VALIDACION DE COLUMNAS NUMERICAS
    # ------------------------------------------------------------

    print("\n" + "=" * 90)
    print("4. VALIDACION DE COLUMNAS QUE DEBERIAN SER NUMERICAS")
    print("=" * 90)

    available_numeric_columns = [
        column
        for column in NUMERIC_CANDIDATES
        if column in columns
    ]

    for column in available_numeric_columns:
        raw_text = as_clean_string(column)
        missing = is_semantic_missing(column)

        number = raw_text.cast(
            pl.Float64,
            strict=False,
        )

        stats = (
            df.select(
                [
                    missing.sum().alias("missing"),
                    (
                        (~missing)
                        & number.is_null()
                    )
                    .sum()
                    .alias("invalid"),
                    (
                        (~missing)
                        & number.is_not_null()
                    )
                    .sum()
                    .alias("valid"),
                ]
            )
            .collect()
            .row(0, named=True)
        )

        print(
            f"{column:<30}"
            f"validos={stats['valid']:>10,}  "
            f"faltantes={stats['missing']:>10,}  "
            f"invalidos={stats['invalid']:>10,}"
        )

    # ------------------------------------------------------------
    # 5. VALIDACION DE FECHA
    # ------------------------------------------------------------

    print("\n" + "=" * 90)
    print("5. VALIDACION DE RELEASED")
    print("=" * 90)

    released_text = as_clean_string("released")
    released_missing = is_semantic_missing("released")

    released_date = released_text.str.strptime(
        pl.Date,
        "%Y-%m-%d",
        strict=False,
    )

    date_stats = (
        df.select(
            [
                released_missing
                .sum()
                .alias("faltantes"),

                (
                    (~released_missing)
                    & released_date.is_not_null()
                )
                .sum()
                .alias("fechas_validas"),

                (
                    (~released_missing)
                    & released_date.is_null()
                )
                .sum()
                .alias("fechas_invalidas"),
            ]
        )
        .collect()
    )

    print(date_stats)

    print("\n" + "=" * 90)
    print("FIN DEL PERFIL")
    print("=" * 90)


if __name__ == "__main__":
    main()
