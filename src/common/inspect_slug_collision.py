from pathlib import Path
import sys

import polars as pl


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


RAW_PATH = Path("data/raw/videogames_data.csv")
CLEAN_PATH = Path("data/processed/videogames_clean.parquet")


def main() -> None:
    print("=" * 100)
    print("DIAGNOSTICO DE SLUGS")
    print("=" * 100)

    clean = pl.scan_parquet(CLEAN_PATH)

    print("\n1. SLUGS NULL DESPUES DE LA LIMPIEZA")
    print("-" * 100)

    null_slugs = (
        clean
        .filter(pl.col("slug").is_null())
        .select(
            "id",
            "name",
            "slug",
        )
        .collect()
    )

    print(f"Cantidad: {null_slugs.height}")
    print(null_slugs)

    print("\n2. SLUGS NO NULL REALMENTE DUPLICADOS")
    print("-" * 100)

    duplicate_non_null = (
        clean
        .filter(pl.col("slug").is_not_null())
        .group_by("slug")
        .agg(
            pl.len().alias("cantidad"),
            pl.col("id").alias("ids"),
            pl.col("name").alias("nombres"),
        )
        .filter(pl.col("cantidad") > 1)
        .sort("cantidad", descending=True)
        .collect()
    )

    print(f"Grupos duplicados no NULL: {duplicate_non_null.height}")
    print(duplicate_non_null)

    if null_slugs.height > 0:
        ids = null_slugs["id"].to_list()

        print("\n3. VALORES ORIGINALES EN EL CSV RAW")
        print("-" * 100)

        raw_rows = (
            pl.scan_csv(
                RAW_PATH,
                infer_schema_length=10000,
            )
            .filter(pl.col("id").is_in(ids))
            .select(
                "id",
                "name",
                "slug",
            )
            .collect()
        )

        print(raw_rows)

    print("\n" + "=" * 100)
    print("FIN DEL DIAGNOSTICO")
    print("=" * 100)


if __name__ == "__main__":
    main()
