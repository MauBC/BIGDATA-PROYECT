from pathlib import Path
import sys
import time

import polars as pl


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


INPUT_PATH = Path("data/raw/videogames_data.csv")
OUTPUT_PATH = Path("data/processed/videogames_clean.parquet")


MISSING_TOKENS = [
    "",
    "nan",
    "null",
    "none",
    "n/a",
    "na",
]


# Estas columnas pueden contener literalmente nombres como
# NULL, None o NaN. No deben tratarse como valores faltantes.
PRESERVE_LITERAL_TEXT_COLUMNS = {
    "name",
    "slug",
}


INTEGER_COLUMNS = [
    "rating_top",
    "ratings_count",
    "reviews_text_count",
    "added",
    "metacritic",
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


FLOAT_COLUMNS = [
    "rating",
    "playtime",
]


def normalized_string(column: str) -> pl.Expr:
    """
    Normaliza strings y convierte tokens que representan ausencia
    de información en NULL real.
    """
    text = (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
    )

    return (
        pl.when(
            text.is_null()
            | text.str.to_lowercase()
            .is_in(MISSING_TOKENS)
            .fill_null(False)
        )
        .then(None)
        .otherwise(text)
        .alias(column)
    )


def main() -> None:
    print("=" * 90)
    print("LIMPIEZA DEL DATASET RAWG")
    print("=" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro el dataset: {INPUT_PATH.resolve()}"
        )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    start = time.perf_counter()

    print(f"\nEntrada : {INPUT_PATH.resolve()}")
    print(f"Salida  : {OUTPUT_PATH.resolve()}")

    # ------------------------------------------------------------------
    # 1. CARGA LAZY
    # ------------------------------------------------------------------

    print("\n[1/7] Leyendo dataset RAW...")

    df = pl.scan_csv(
        INPUT_PATH,
        infer_schema_length=10000,
    )

    schema = df.collect_schema()

    # ------------------------------------------------------------------
    # 2. NORMALIZAR STRINGS
    # ------------------------------------------------------------------

    print("[2/7] Normalizando valores faltantes...")

    string_columns = [
        column
        for column, dtype in schema.items()
        if (
            dtype == pl.String
            and column not in PRESERVE_LITERAL_TEXT_COLUMNS
        )
    ]

    df = df.with_columns(
        [
            normalized_string(column)
            for column in string_columns
        ]
    )

    # ------------------------------------------------------------------
    # 3. TIPOS NUMERICOS
    # ------------------------------------------------------------------

    print("[3/7] Convirtiendo columnas numericas...")

    available_columns = set(df.collect_schema().names())

    integer_expressions = []

    for column in INTEGER_COLUMNS:
        if column in available_columns:
            integer_expressions.append(
                pl.col(column)
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
                .alias(column)
            )

    float_expressions = []

    for column in FLOAT_COLUMNS:
        if column in available_columns:
            float_expressions.append(
                pl.col(column)
                .cast(pl.Float64, strict=False)
                .alias(column)
            )

    df = df.with_columns(
        integer_expressions
        + float_expressions
    )

    # ------------------------------------------------------------------
    # 4. FECHA
    # ------------------------------------------------------------------

    print("[4/7] Transformando fecha released...")

    df = df.with_columns(
        pl.col("released")
        .str.strptime(
            pl.Date,
            "%Y-%m-%d",
            strict=False,
        )
        .alias("released")
    )

    df = df.with_columns(
        pl.col("released")
        .dt.year()
        .cast(pl.Int32)
        .alias("release_year")
    )

    # ------------------------------------------------------------------
    # 5. ESRB
    # ------------------------------------------------------------------

    print("[5/7] Extrayendo clasificacion ESRB...")

    df = df.with_columns(
        pl.col("esrb_rating")
        .str.json_path_match("$.name")
        .alias("esrb_rating_name")
    )

    # ------------------------------------------------------------------
    # 6. VARIABLES DERIVADAS
    # ------------------------------------------------------------------

    print("[6/7] Creando variables derivadas...")

    df = df.with_columns(
        [
            pl.when(pl.col("genres").is_null())
            .then(0)
            .otherwise(
                pl.col("genres").str.count_matches(r"\|") + 1
            )
            .cast(pl.Int32)
            .alias("genres_count"),

            pl.when(pl.col("platforms").is_null())
            .then(0)
            .otherwise(
                pl.col("platforms").str.count_matches(r"\|") + 1
            )
            .cast(pl.Int32)
            .alias("platforms_count"),

            pl.when(pl.col("developers").is_null())
            .then(0)
            .otherwise(
                pl.col("developers").str.count_matches(r"\|") + 1
            )
            .cast(pl.Int32)
            .alias("developers_count_clean"),

            pl.when(pl.col("publishers").is_null())
            .then(0)
            .otherwise(
                pl.col("publishers").str.count_matches(r"\|") + 1
            )
            .cast(pl.Int32)
            .alias("publishers_count"),
        ]
    )

    # ------------------------------------------------------------------
    # 7. EXPORTAR PARQUET
    # ------------------------------------------------------------------

    print("[7/7] Generando Parquet...")

    df.sink_parquet(
        OUTPUT_PATH,
        compression="zstd",
    )

    elapsed = time.perf_counter() - start

    print("\n" + "=" * 90)
    print("DATASET LIMPIO GENERADO")
    print("=" * 90)

    clean = pl.scan_parquet(OUTPUT_PATH)

    clean_schema = clean.collect_schema()

    row_count = (
        clean
        .select(pl.len())
        .collect()
        .item()
    )

    output_mb = (
        OUTPUT_PATH.stat().st_size
        / (1024 ** 2)
    )

    print(f"Filas       : {row_count:,}")
    print(f"Columnas    : {len(clean_schema)}")
    print(f"Tamaño      : {output_mb:,.2f} MB")
    print(f"Tiempo      : {elapsed:,.2f} segundos")

    print("\nNuevas columnas:")
    print("  - release_year")
    print("  - esrb_rating_name")
    print("  - genres_count")
    print("  - platforms_count")
    print("  - developers_count_clean")
    print("  - publishers_count")

    print("\n" + "=" * 90)
    print("[OK] Limpieza finalizada")
    print("=" * 90)


if __name__ == "__main__":
    main()

