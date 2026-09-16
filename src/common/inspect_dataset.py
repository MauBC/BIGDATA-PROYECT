from pathlib import Path

import polars as pl


DATASET_PATH = Path("data/raw/videogames_data.csv")


def main() -> None:
    print("=" * 80)
    print("INSPECCION DEL DATASET RAWG")
    print("=" * 80)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro el dataset en: {DATASET_PATH.resolve()}"
        )

    size_bytes = DATASET_PATH.stat().st_size
    size_mb = size_bytes / (1024 ** 2)

    print(f"\nArchivo: {DATASET_PATH.name}")
    print(f"Ruta: {DATASET_PATH.resolve()}")
    print(f"Tamaño: {size_mb:,.2f} MB")

    print("\nLeyendo metadata del CSV...")

    lazy_df = pl.scan_csv(
        DATASET_PATH,
        infer_schema_length=10000,
    )

    schema = lazy_df.collect_schema()

    row_count = (
        lazy_df
        .select(pl.len().alias("rows"))
        .collect()
        .item()
    )

    print("\n" + "=" * 80)
    print("DIMENSIONES")
    print("=" * 80)

    print(f"Filas: {row_count:,}")
    print(f"Columnas: {len(schema)}")

    print("\n" + "=" * 80)
    print("COLUMNAS Y TIPOS")
    print("=" * 80)

    for index, (column, dtype) in enumerate(schema.items(), start=1):
        print(f"{index:>2}. {column:<30} {dtype}")

    print("\n" + "=" * 80)
    print("VALORES NULOS")
    print("=" * 80)

    null_df = lazy_df.select(
        [
            pl.col(column).null_count().alias(column)
            for column in schema.names()
        ]
    ).collect()

    null_counts = null_df.row(0, named=True)

    null_counts_sorted = sorted(
        null_counts.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    for column, null_count in null_counts_sorted:
        percentage = (
            (null_count / row_count) * 100
            if row_count > 0
            else 0
        )

        print(
            f"{column:<30} "
            f"{null_count:>10,} "
            f"({percentage:>6.2f}%)"
        )

    print("\n" + "=" * 80)
    print("PRIMERAS 5 FILAS")
    print("=" * 80)

    print(
        lazy_df
        .head(5)
        .collect()
    )

    print("\n" + "=" * 80)

    if row_count >= 500_000:
        print(
            f"[OK] El dataset cumple el requisito de >= 500,000 registros: "
            f"{row_count:,}"
        )
    else:
        print(
            f"[ERROR] El dataset tiene solo {row_count:,} registros."
        )

    print("=" * 80)


if __name__ == "__main__":
    main()
