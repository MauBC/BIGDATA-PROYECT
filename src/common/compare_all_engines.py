from __future__ import annotations

from pathlib import Path
import math
import sys

import pandas as pd


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


BASE = Path("results")

DIRECTORIES = {
    "Polars": BASE / "polars",
    "Dask": BASE / "dask",
    "Spark": BASE / "spark",
    "Modin": BASE / "modin",
}

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
        return False, {
            "motivo": "COLUMNAS DIFERENTES",
            "referencia": list(reference.columns),
            "candidato": list(candidate.columns),
        }

    if len(reference) != len(candidate):
        return False, {
            "motivo": "NUMERO DE FILAS DIFERENTE",
            "referencia": len(reference),
            "candidato": len(candidate),
        }

    reference = reference.copy()
    candidate = candidate.copy()

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

        for index in range(len(reference)):

            a = normalize_missing(
                reference.iloc[index][column]
            )

            b = normalize_missing(
                candidate.iloc[index][column]
            )

            if a is None and b is None:
                continue

            numeric_ok = False

            try:
                av = float(a)
                bv = float(b)

                if math.isclose(
                    av,
                    bv,
                    rel_tol=0.0,
                    abs_tol=TOLERANCE,
                ):
                    numeric_ok = True
            except (TypeError, ValueError):
                pass

            if numeric_ok:
                continue

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
                return False, differences

    if differences:
        return False, differences

    return True, None


def main():

    print("=" * 115)
    print(
        "VALIDACION FINAL - "
        "POLARS VS DASK VS SPARK VS MODIN"
    )
    print("=" * 115)

    totals = {
        "Dask": 0,
        "Spark": 0,
        "Modin": 0,
    }

    errors = {
        "Dask": 0,
        "Spark": 0,
        "Modin": 0,
    }

    for number, filename in enumerate(
        FILES,
        start=1,
    ):

        print()
        print(
            f"Q{number:02d} - {filename}"
        )
        print("-" * 115)

        reference = pd.read_csv(
            DIRECTORIES["Polars"] / filename
        )

        for engine in [
            "Dask",
            "Spark",
            "Modin",
        ]:

            candidate = pd.read_csv(
                DIRECTORIES[engine] / filename
            )

            ok, details = compare_frames(
                reference,
                candidate,
            )

            status = (
                "[OK]"
                if ok
                else "[ERROR]"
            )

            print(
                f"Polars vs {engine:<5}: "
                f"{status}"
            )

            if ok:
                totals[engine] += 1
            else:
                errors[engine] += 1

                print(
                    f"  Diferencias detectadas "
                    f"en {engine}:"
                )

                if isinstance(
                    details,
                    list,
                ):
                    for diff in details:
                        print(
                            "  "
                            f"fila={diff['fila']} "
                            f"columna={diff['columna']} "
                            f"Polars={diff['referencia']} "
                            f"{engine}={diff['candidato']}"
                        )
                else:
                    print(
                        f"  {details}"
                    )

    print()
    print("=" * 115)
    print("RESUMEN FINAL")
    print("=" * 115)

    for engine in [
        "Dask",
        "Spark",
        "Modin",
    ]:
        print(
            f"{engine:<5} equivalentes : "
            f"{totals[engine]}/10"
        )

        print(
            f"{engine:<5} diferentes   : "
            f"{errors[engine]}/10"
        )

    print()
    print(
        f"Tolerancia numerica: "
        f"{TOLERANCE}"
    )

    total_errors = sum(
        errors.values()
    )

    if total_errors == 0:

        print()
        print(
            "[OK] LOS CUATRO MOTORES "
            "PRODUCEN RESULTADOS EQUIVALENTES"
        )

        print(
            "[OK] POLARS 10/10"
        )

        print(
            "[OK] DASK   10/10"
        )

        print(
            "[OK] SPARK  10/10"
        )

        print(
            "[OK] MODIN  10/10"
        )

    else:

        print()
        print(
            "[REVISAR] EXISTEN DIFERENCIAS "
            "ENTRE LOS MOTORES"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
