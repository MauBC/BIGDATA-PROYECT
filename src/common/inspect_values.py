from pathlib import Path
import sys

import polars as pl


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DATASET_PATH = Path("data/raw/videogames_data.csv")

MISSING_TOKENS = [
    "",
    "nan",
    "null",
    "none",
    "n/a",
    "na",
]

COMPLEX_COLUMNS = [
    "platforms",
    "stores",
    "developers",
    "genres",
    "tags",
    "publishers",
    "esrb_rating",
    "added_by_status",
    "ratings",
    "parent_platforms",
    "metacritic_platforms",
    "alternative_names",
    "tba",
]

NUMERIC_COLUMNS = [
    "rating",
    "rating_top",
    "ratings_count",
    "reviews_text_count",
    "added",
    "metacritic",
    "playtime",
    "suggestions_count",
    "reviews_count",
]


def clean_text(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
    )


def valid_text(column: str) -> pl.Expr:
    text = clean_text(column)

    return (
        text.is_not_null()
        & ~text.str.to_lowercase().is_in(MISSING_TOKENS)
    )


def main() -> None:
    print("=" * 100)
    print("INSPECCION DE VALORES DEL DATASET RAWG")
    print("=" * 100)

    df = pl.scan_csv(
        DATASET_PATH,
        infer_schema_length=10000,
    )

    schema = df.collect_schema()
    available_columns = set(schema.names())

    # ------------------------------------------------------------------
    # 1. COLUMNAS COMPLEJAS
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("1. EJEMPLOS DE COLUMNAS COMPLEJAS")
    print("=" * 100)

    for column in COMPLEX_COLUMNS:
        if column not in available_columns:
            continue

        print(f"\n--- {column} ---")

        sample = (
            df
            .filter(valid_text(column))
            .select(clean_text(column).alias(column))
            .unique()
            .head(5)
            .collect()
        )

        for value in sample[column].to_list():
            value_str = str(value)

            if len(value_str) > 500:
                value_str = value_str[:500] + "..."

            print(value_str)

    # ------------------------------------------------------------------
    # 2. RANGOS NUMERICOS
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("2. RANGOS DE COLUMNAS NUMERICAS")
    print("=" * 100)

    for column in NUMERIC_COLUMNS:
        if column not in available_columns:
            continue

        numeric = (
            clean_text(column)
            .cast(pl.Float64, strict=False)
        )

        result = (
            df.select(
                numeric.min().alias("min"),
                numeric.max().alias("max"),
                numeric.mean().alias("mean"),
                numeric.median().alias("median"),
            )
            .collect()
            .row(0, named=True)
        )

        print(
            f"{column:<25} "
            f"min={result['min']}  "
            f"max={result['max']}  "
            f"mean={result['mean']}  "
            f"median={result['median']}"
        )

    # ------------------------------------------------------------------
    # 3. RELEASED
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("3. RANGO DE FECHAS")
    print("=" * 100)

    released = (
        clean_text("released")
        .str.strptime(
            pl.Date,
            "%Y-%m-%d",
            strict=False,
        )
    )

    date_stats = (
        df.select(
            released.min().alias("fecha_minima"),
            released.max().alias("fecha_maxima"),
        )
        .collect()
    )

    print(date_stats)

    # ------------------------------------------------------------------
    # 4. VALORES DE TBA
    # ------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("4. VALORES DISTINTOS DE TBA")
    print("=" * 100)

    tba_values = (
        df
        .filter(valid_text("tba"))
        .select(
            clean_text("tba")
            .str.to_lowercase()
            .alias("tba")
        )
        .group_by("tba")
        .len()
        .sort("len", descending=True)
        .collect()
    )

    print(tba_values)

    print("\n" + "=" * 100)
    print("FIN DE INSPECCION")
    print("=" * 100)


if __name__ == "__main__":
    main()
