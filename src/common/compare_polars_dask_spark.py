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
SPARK_DIR = Path("results/spark")

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

TOLERANCE = 0.0001


def normalize_missing(value):
    if pd.isna(value):
        return None
    return value


def compare_frames(reference, candidate):
    if list(reference.columns) != list(candidate.columns):
        return False, "COLUMNAS DIFERENTES"

    if len(reference) != len(candidate):
        return False, "NUMERO DE FILAS DIFERENTE"

    reference = reference.copy()
    candidate = candidate.copy()

    # Orden determinista para poder comparar aunque los motores
    # devuelvan físicamente las filas en distinto orden.
    sort_columns = list(reference.columns)

    reference = (
        reference
        .sort_values(
            sort_columns,
            na_position="last",
        )
        .reset_index(drop=True)
    )

    candidate = (
        candidate
        .sort_values(
            sort_columns,
            na_position="last",
        )
        .reset_index(drop=True)
    )

    differences = []

    for column in reference.columns:

        ref = reference[column]
        cand = candidate[column]

        ref_numeric = pd.to_numeric(
            ref,
            errors="coerce",
        )

        cand_numeric = pd.to_numeric(
            cand,
            errors="coerce",
        )

        numeric_mask = (
            ref_numeric.notna()
            | cand_numeric.notna()
        )

        for index in range(len(reference)):

            a = normalize_missing(ref.iloc[index])
            b = normalize_missing(cand.iloc[index])

            if a is None and b is None:
                continue

            if numeric_mask.iloc[index]:
                try:
                    av = float(a)
                    bv = float(b)

                    if math.isclose(
                        av,
                        bv,
                        rel_tol=0.0,
                        abs_tol=TOLERANCE,
                    ):
                        continue
                except (TypeError, ValueError):
                    pass
            else:
                if str(a).strip() == str(b).strip():
                    continue

            differences.append(
                {
                    "fila": index,
                    "columna": column,
                    "referencia": a,
                    "candidato": b,
                }
            )

            if len(differences) >= 10:
                break

        if len(differences) >= 10:
            break

    if differences:
        return False, differences

    return True, None


def main():

    print("=" * 105)
    print("VALIDACION POLARS VS DASK VS SPARK")
    print("=" * 105)

    spark_ok = 0
    spark_error = 0

    for number, filename in enumerate(
        FILES,
        start=1,
    ):

        print()
        print(f"Q{number:02d} - {filename}")
        print("-" * 105)

        polars = pd.read_csv(
            POLARS_DIR / filename
        )

        dask = pd.read_csv(
            DASK_DIR / filename
        )

        spark = pd.read_csv(
            SPARK_DIR / filename
        )

        pd_ok, pd_details = compare_frames(
            polars,
            dask,
        )

        ps_ok, ps_details = compare_frames(
            polars,
            spark,
        )

        print(
            f"Polars vs Dask  : "
            f"{'[OK]' if pd_ok else '[ERROR]'}"
        )

        print(
            f"Polars vs Spark : "
            f"{'[OK]' if ps_ok else '[ERROR]'}"
        )

        if ps_ok:
            spark_ok += 1
        else:
            spark_error += 1

            print("\nDiferencias Spark:")

            if isinstance(ps_details, list):
                for diff in ps_details:
                    print(
                        f"  fila={diff['fila']} "
                        f"columna={diff['columna']} "
                        f"Polars={diff['referencia']} "
                        f"Spark={diff['candidato']}"
                    )
            else:
                print(f"  {ps_details}")

    print()
    print("=" * 105)
    print("RESUMEN")
    print("=" * 105)

    print(
        f"Spark equivalentes : "
        f"{spark_ok}/10"
    )

    print(
        f"Spark diferentes   : "
        f"{spark_error}/10"
    )

    print(
        f"Tolerancia numerica: "
        f"{TOLERANCE}"
    )

    if spark_error == 0:
        print(
            "\n[OK] POLARS, DASK Y SPARK "
            "PRODUCEN RESULTADOS EQUIVALENTES"
        )
    else:
        print(
            "\n[REVISAR] EXISTEN DIFERENCIAS "
            "ENTRE LOS MOTORES"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
