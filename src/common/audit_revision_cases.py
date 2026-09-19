from __future__ import annotations

from pathlib import Path

import polars as pl


DATASET = Path(
    "data/processed/videogames_clean.parquet"
)

DIMENSIONS = {
    "genres": "genres_count",
    "platforms": "platforms_count",
    "developers": "developers_count_clean",
    "publishers": "publishers_count",
}


def separator():
    print("=" * 100)


def normalized_count_expr(
    column: str,
) -> pl.Expr:
    """
    Cuenta elementos válidos y únicos de una dimensión
    separada por |.

    Reglas:
    - dimensión NULL -> 0
    - trim de elementos
    - elimina elementos vacíos
    - elimina elementos repetidos
    """

    normalized = (
        pl.col(column)
        .str.split("|")
        .list.eval(
            pl.when(
                pl.element()
                .str.strip_chars()
                != ""
            )
            .then(
                pl.element()
                .str.strip_chars()
            )
            .otherwise(None)
        )
        .list.drop_nulls()
        .list.unique()
        .list.len()
    )

    return (
        pl.when(
            pl.col(column).is_null()
        )
        .then(pl.lit(0))
        .otherwise(normalized)
        .cast(pl.Int32)
    )


def main():
    separator()
    print("AUDITORIA REVISION FINAL - DATASET RAWG")
    separator()

    if not DATASET.exists():
        raise SystemExit(
            f"No existe: {DATASET}"
        )

    lf = pl.scan_parquet(DATASET)

    schema = lf.collect_schema()

    total = (
        lf.select(
            pl.len().alias("filas")
        )
        .collect()
        .item()
    )

    print()
    print(f"Dataset : {DATASET}")
    print(f"Filas   : {total:,}")
    print(f"Columnas: {len(schema.names())}")

    separator()
    print("1. NAN NUMERICOS")
    separator()

    float_columns = [
        name
        for name, dtype
        in zip(
            schema.names(),
            schema.dtypes(),
        )
        if dtype in (
            pl.Float32,
            pl.Float64,
        )
    ]

    nan_expressions = []

    for column in float_columns:
        nan_expressions.append(
            pl.col(column)
            .is_nan()
            .fill_null(False)
            .sum()
            .alias(column)
        )

    if nan_expressions:
        nan_result = (
            lf.select(nan_expressions)
            .collect()
        )

        for column in nan_result.columns:
            value = nan_result[column][0]

            print(
                f"{column}: "
                f"{int(value):,}"
            )
    else:
        print(
            "No se encontraron columnas float."
        )

    separator()
    print("2. RATING FUERA DE RANGO")
    separator()

    rating_invalid = (
        lf.filter(
            pl.col("rating").is_not_null()
            & (
                (pl.col("rating") < 0)
                | (pl.col("rating") > 5)
                | pl.col("rating")
                    .is_nan()
                    .fill_null(False)
            )
        )
        .select(pl.len())
        .collect()
        .item()
    )

    print(
        f"rating inválido / NaN: "
        f"{rating_invalid:,}"
    )

    separator()
    print("3. RELEASE_YEAR")
    separator()

    release_inconsistent = (
        lf.filter(
            (
                pl.col("released").is_not_null()
                & pl.col("release_year").is_null()
            )
            |
            (
                pl.col("released").is_null()
                & pl.col("release_year").is_not_null()
            )
            |
            (
                pl.col("released").is_not_null()
                & pl.col("release_year").is_not_null()
                & (
                    pl.col("release_year")
                    != pl.col("released").dt.year()
                )
            )
        )
        .select(pl.len())
        .collect()
        .item()
    )

    print(
        "Inconsistencias "
        f"released/release_year: "
        f"{release_inconsistent:,}"
    )

    separator()
    print("4. DIMENSIONES")
    separator()

    total_dimension_issues = 0

    for dimension, count_column in (
        DIMENSIONS.items()
    ):
        print()
        print(
            f"--- {dimension} / "
            f"{count_column} ---"
        )

        empty_token_rows = (
            lf.filter(
                pl.col(dimension)
                .is_not_null()
                & pl.col(dimension)
                .str.contains(
                    r"(^\s*\|)|(\|\s*$)|(\|\s*\|)"
                )
                .fill_null(False)
            )
            .select(pl.len())
            .collect()
            .item()
        )

        normalized_count = (
            normalized_count_expr(
                dimension
            )
        )

        count_mismatch = (
            lf.filter(
                (
                    pl.col(count_column)
                    .fill_null(-1)
                )
                != (
                    normalized_count
                    .fill_null(-1)
                )
            )
            .select(pl.len())
            .collect()
            .item()
        )

        pairs = (
            lf.select(
                "id",
                pl.col(dimension)
                .str.split("|")
                .alias("value"),
            )
            .explode("value", empty_as_null=True)
            .with_columns(
                pl.col("value")
                .str.strip_chars()
            )
            .filter(
                pl.col("value").is_not_null()
                & (
                    pl.col("value") != ""
                )
            )
        )

        duplicate_pairs = (
            pairs
            .group_by(
                [
                    "id",
                    "value",
                ]
            )
            .len()
            .filter(
                pl.col("len") > 1
            )
            .select(
                pl.len()
                .alias(
                    "duplicate_pairs"
                )
            )
            .collect()
            .item()
        )

        print(
            "Filas con elementos vacíos : "
            f"{empty_token_rows:,}"
        )

        print(
            "Conteos derivados distintos : "
            f"{count_mismatch:,}"
        )

        print(
            "Pares id-dim repetidos      : "
            f"{duplicate_pairs:,}"
        )

        total_dimension_issues += (
            empty_token_rows
            + count_mismatch
            + duplicate_pairs
        )

    separator()
    print("5. RESUMEN")
    separator()

    problems = (
        rating_invalid
        + release_inconsistent
        + total_dimension_issues
    )

    print(
        "Problemas estructurales "
        f"detectados: {problems:,}"
    )

    print()

    if problems == 0:
        print(
            "[OK] No se encontraron los "
            "casos estructurales revisados."
        )
    else:
        print(
            "[INFO] Hay casos reales que "
            "deben revisarse antes de "
            "regenerar resultados."
        )


if __name__ == "__main__":
    main()
