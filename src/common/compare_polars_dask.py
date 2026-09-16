from collections import Counter
from pathlib import Path
import math
import sys

import numpy as np
import pandas as pd


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


POLARS_DIR = Path("results/polars")
DASK_DIR = Path("results/dask")


FILES = [
    "q01_resumen_general.csv",
    "q02_videojuegos_por_anio.csv",
    "q03_top_generos.csv",
    "q04_top_plataformas.csv",
    "q05_top_desarrolladores.csv",
    "q06_rating_por_genero.csv",
    "q07_metacritic_por_genero.csv",
    "q08_top_videojuegos_ratings.csv",
    "q09_playtime_por_genero.csv",
    "q10_evolucion_anual.csv",
]


def normalize_value(value):
    if pd.isna(value):
        return "<NULL>"

    if isinstance(
        value,
        (
            int,
            float,
            np.integer,
            np.floating,
        ),
    ):
        number = float(value)

        if math.isclose(
            number,
            round(number),
            abs_tol=1e-9,
        ):
            return float(round(number))

        return round(number, 4)

    return str(value).strip()


def dataframe_counter(df):
    normalized_rows = []

    for row in df.itertuples(
        index=False,
        name=None,
    ):
        normalized_rows.append(
            tuple(
                normalize_value(value)
                for value in row
            )
        )

    return Counter(normalized_rows)


def main():
    print("=" * 100)
    print("VALIDACION POLARS VS DASK")
    print("=" * 100)

    total_ok = 0
    total_error = 0

    for index, filename in enumerate(
        FILES,
        start=1,
    ):
        polars_path = POLARS_DIR / filename
        dask_path = DASK_DIR / filename

        print(
            f"\nQ{index:02d} - {filename}"
        )
        print("-" * 100)

        if not polars_path.exists():
            print(
                f"[ERROR] No existe Polars: "
                f"{polars_path}"
            )
            total_error += 1
            continue

        if not dask_path.exists():
            print(
                f"[ERROR] No existe Dask: "
                f"{dask_path}"
            )
            total_error += 1
            continue

        polars_df = pd.read_csv(polars_path)
        dask_df = pd.read_csv(dask_path)

        if list(polars_df.columns) != list(
            dask_df.columns
        ):
            print("[ERROR] Columnas diferentes")

            print(
                "Polars:",
                list(polars_df.columns),
            )

            print(
                "Dask:",
                list(dask_df.columns),
            )

            total_error += 1
            continue

        if len(polars_df) != len(dask_df):
            print(
                "[ERROR] Cantidad de filas diferente"
            )

            print(
                f"Polars: {len(polars_df)}"
            )

            print(
                f"Dask:   {len(dask_df)}"
            )

            total_error += 1
            continue

        polars_rows = dataframe_counter(
            polars_df
        )

        dask_rows = dataframe_counter(
            dask_df
        )

        if polars_rows == dask_rows:
            print(
                f"[OK] Coinciden "
                f"{len(polars_df):,} filas"
            )

            total_ok += 1
        else:
            print(
                "[ERROR] Los resultados "
                "no son equivalentes"
            )

            only_polars = (
                polars_rows - dask_rows
            )

            only_dask = (
                dask_rows - polars_rows
            )

            print(
                "\nEjemplos solo en Polars:"
            )

            for row, count in list(
                only_polars.items()
            )[:5]:
                print(
                    f"  {row} x{count}"
                )

            print(
                "\nEjemplos solo en Dask:"
            )

            for row, count in list(
                only_dask.items()
            )[:5]:
                print(
                    f"  {row} x{count}"
                )

            total_error += 1

    print("\n" + "=" * 100)
    print("RESUMEN")
    print("=" * 100)

    print(
        f"Consultas correctas : "
        f"{total_ok}/10"
    )

    print(
        f"Consultas diferentes: "
        f"{total_error}/10"
    )

    print("=" * 100)

    if total_error == 0:
        print(
            "[OK] POLARS Y DASK PRODUCEN "
            "RESULTADOS EQUIVALENTES"
        )
    else:
        print(
            "[ERROR] EXISTEN DIFERENCIAS "
            "QUE DEBEN REVISARSE"
        )
        sys.exit(1)

    print("=" * 100)


if __name__ == "__main__":
    main()
