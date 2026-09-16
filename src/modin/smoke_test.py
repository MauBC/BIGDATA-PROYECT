from __future__ import annotations

import os
import sys
import time

# Debe definirse ANTES de importar modin.pandas.
os.environ["MODIN_ENGINE"] = "ray"

import modin
import modin.pandas as pd
import pandas
import pyarrow
import ray


DATASET = "data/processed/videogames_clean.parquet"


def main() -> None:

    print("=" * 90)
    print("MODIN + RAY - SMOKE TEST")
    print("=" * 90)

    print(f"Python  : {sys.version.split()[0]}")
    print(f"Modin   : {modin.__version__}")
    print(f"Ray     : {ray.__version__}")
    print(f"pandas  : {pandas.__version__}")
    print(f"PyArrow : {pyarrow.__version__}")
    print(f"Engine  : {os.environ.get('MODIN_ENGINE')}")

    print()
    print(f"Dataset : {DATASET}")

    start = time.perf_counter()

    df = pd.read_parquet(DATASET)

    total = len(df)
    unique_ids = int(df["id"].nunique())
    min_year = int(df["release_year"].min())
    max_year = int(df["release_year"].max())

    elapsed = time.perf_counter() - start

    print()
    print(f"Filas       : {total:,}")
    print(f"Columnas    : {len(df.columns)}")
    print(f"IDs unicos  : {unique_ids:,}")
    print(f"Anio minimo : {min_year}")
    print(f"Anio maximo : {max_year}")
    print(f"Tiempo      : {elapsed:.4f} segundos")

    print()
    print("Primeras 5 filas:")

    print(
        df[
            [
                "id",
                "name",
                "release_year",
                "genres",
                "platforms",
            ]
        ].head(5)
    )

    print()
    print("=" * 90)

    expected = (
        total == 899_585
        and unique_ids == 899_585
        and min_year == 1954
        and max_year == 2033
        and len(df.columns) == 60
    )

    if not expected:
        print("[ERROR] LOS RESULTADOS NO COINCIDEN")
        raise SystemExit(1)

    print(
        "[OK] MODIN + RAY LEE CORRECTAMENTE "
        "EL DATASET PROCESADO"
    )
    print("=" * 90)

    if ray.is_initialized():
        ray.shutdown()


if __name__ == "__main__":
    main()
