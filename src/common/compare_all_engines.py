from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pandas as pd


RESULTS = Path("results")

ENGINES = (
    "polars",
    "dask",
    "spark",
    "modin",
)

REFERENCE = "polars"

TOLERANCE = 0.0001

QUERIES = tuple(
    f"q{i:02d}"
    for i in range(1, 11)
)


def find_query_file(
    engine: str,
    query: str,
) -> Path:
    directory = RESULTS / engine

    matches = sorted(
        directory.glob(
            f"{query}_*.csv"
        )
    )

    if len(matches) == 0:
        raise FileNotFoundError(
            f"No se encontró {query} "
            f"para {engine} en "
            f"{directory}"
        )

    if len(matches) > 1:
        names = ", ".join(
            str(path)
            for path in matches
        )

        raise RuntimeError(
            f"Hay múltiples archivos para "
            f"{engine}/{query}: {names}"
        )

    return matches[0]


def read_result(
    path: Path,
) -> pd.DataFrame:
    """
    Conserva literales como:
    NULL
    None
    NaN

    Solo una celda realmente vacía
    se interpreta como faltante.
    """

    return pd.read_csv(
        path,
        keep_default_na=False,
        na_values=[""],
    )


def normalized_numeric(
    series: pd.Series,
):
    """
    Devuelve una Serie numérica cuando
    todos los valores no nulos se pueden
    interpretar como números.

    En caso contrario devuelve None.
    """

    if pd.api.types.is_numeric_dtype(
        series
    ):
        return pd.to_numeric(
            series,
            errors="coerce",
        )

    non_null = (
        series
        .dropna()
    )

    if non_null.empty:
        return None

    converted = pd.to_numeric(
        non_null,
        errors="coerce",
    )

    if converted.notna().all():
        return pd.to_numeric(
            series,
            errors="coerce",
        )

    return None


def compare_numeric(
    left: pd.Series,
    right: pd.Series,
):
    left_num = normalized_numeric(left)
    right_num = normalized_numeric(right)

    if (
        left_num is None
        or right_num is None
    ):
        return None

    left_values = (
        left_num
        .astype(float)
        .to_numpy()
    )

    right_values = (
        right_num
        .astype(float)
        .to_numpy()
    )

    equal = np.isclose(
        left_values,
        right_values,
        rtol=0.0,
        atol=TOLERANCE,
        equal_nan=True,
    )

    return equal


def compare_text(
    left: pd.Series,
    right: pd.Series,
):
    left_missing = left.isna()
    right_missing = right.isna()

    both_missing = (
        left_missing
        & right_missing
    )

    one_missing = (
        left_missing
        ^ right_missing
    )

    equal = (
        left.astype("string")
        == right.astype("string")
    ).fillna(False)

    result = (
        both_missing
        | (
            (~one_missing)
            & equal
        )
    )

    return result.to_numpy()


def compare_dataframes(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
):
    problems = []

    if list(expected.columns) != list(
        actual.columns
    ):
        problems.append(
            "Columnas diferentes. "
            f"Esperado={list(expected.columns)}; "
            f"actual={list(actual.columns)}"
        )

        return problems

    if len(expected) != len(actual):
        problems.append(
            "Cantidad de filas diferente. "
            f"Esperado={len(expected)}; "
            f"actual={len(actual)}"
        )

        return problems

    for column in expected.columns:
        left = expected[column]
        right = actual[column]

        numeric_equal = compare_numeric(
            left,
            right,
        )

        if numeric_equal is not None:
            equal = numeric_equal
        else:
            equal = compare_text(
                left,
                right,
            )

        invalid = np.flatnonzero(
            ~equal
        )

        if len(invalid) == 0:
            continue

        row = int(
            invalid[0]
        )

        problems.append(
            f"Columna '{column}', "
            f"fila {row}: "
            f"esperado={left.iloc[row]!r}; "
            f"actual={right.iloc[row]!r}"
        )

    return problems


def check_ascending(
    df: pd.DataFrame,
    columns,
):
    ordered = (
        df.sort_values(
            list(columns),
            ascending=True,
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )

    return df.reset_index(
        drop=True
    ).equals(ordered)


def check_rank_order(
    df: pd.DataFrame,
    metric: str,
    dimension: str,
):
    expected = (
        df.sort_values(
            [metric, dimension],
            ascending=[False, True],
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )

    return (
        df.reset_index(drop=True)
        .equals(expected)
    )


def validate_contract(
    query: str,
    df: pd.DataFrame,
):
    """
    Valida el orden contractual sin
    modificar el DataFrame.
    """

    try:
        if query == "q02":
            ok = check_ascending(
                df,
                ["release_year"],
            )

        elif query == "q03":
            ok = check_rank_order(
                df,
                "videojuegos",
                "genres",
            )

        elif query == "q04":
            ok = check_rank_order(
                df,
                "videojuegos",
                "platforms",
            )

        elif query == "q05":
            ok = check_rank_order(
                df,
                "videojuegos",
                "developers",
            )

        elif query == "q06":
            ok = check_rank_order(
                df,
                "rating_promedio",
                "genres",
            )

        elif query == "q07":
            ok = check_rank_order(
                df,
                "metacritic_promedio",
                "genres",
            )

        elif query == "q08":
            expected = (
                df.sort_values(
                    [
                        "ratings_count",
                        "id",
                    ],
                    ascending=[
                        False,
                        True,
                    ],
                    kind="stable",
                    na_position="last",
                )
                .reset_index(drop=True)
            )

            ok = (
                df.reset_index(drop=True)
                .equals(expected)
            )

        elif query == "q09":
            ok = check_rank_order(
                df,
                "playtime_promedio",
                "genres",
            )

        elif query == "q10":
            ok = check_ascending(
                df,
                ["release_year"],
            )

        else:
            ok = True

    except KeyError as exc:
        return (
            False,
            f"Falta columna contractual: {exc}",
        )

    if not ok:
        return (
            False,
            "El resultado no respeta "
            "el orden definido en el contrato.",
        )

    return (
        True,
        None,
    )


def main():
    print("=" * 110)
    print(
        "COMPARACION ESTRICTA "
        "POLARS / DASK / SPARK / MODIN"
    )
    print("=" * 110)

    totals = {
        engine: 0
        for engine in ENGINES
        if engine != REFERENCE
    }

    failures = []

    for query in QUERIES:
        print()
        print("-" * 110)
        print(query.upper())
        print("-" * 110)

        reference_path = find_query_file(
            REFERENCE,
            query,
        )

        reference_df = read_result(
            reference_path
        )

        contract_ok, reason = (
            validate_contract(
                query,
                reference_df,
            )
        )

        if contract_ok:
            print(
                f"{REFERENCE:<8} contrato : [OK]"
            )
        else:
            print(
                f"{REFERENCE:<8} contrato : "
                f"[ERROR] {reason}"
            )

            failures.append(
                f"{query}/{REFERENCE}: "
                f"{reason}"
            )

        for engine in ENGINES:
            if engine == REFERENCE:
                continue

            path = find_query_file(
                engine,
                query,
            )

            df = read_result(path)

            engine_contract_ok, (
                engine_contract_reason
            ) = validate_contract(
                query,
                df,
            )

            problems = compare_dataframes(
                reference_df,
                df,
            )

            equivalent = (
                not problems
                and contract_ok
                and engine_contract_ok
            )

            if equivalent:
                totals[engine] += 1

                print(
                    f"Polars vs "
                    f"{engine.capitalize():<7}: "
                    "[OK]"
                )

            else:
                print(
                    f"Polars vs "
                    f"{engine.capitalize():<7}: "
                    "[ERROR]"
                )

                if not engine_contract_ok:
                    print(
                        "  Contrato: "
                        f"{engine_contract_reason}"
                    )

                    failures.append(
                        f"{query}/{engine}: "
                        f"{engine_contract_reason}"
                    )

                for problem in problems[:5]:
                    print(
                        f"  {problem}"
                    )

                    failures.append(
                        f"{query}/"
                        f"{engine}: "
                        f"{problem}"
                    )

    print()
    print("=" * 110)
    print("RESUMEN FINAL")
    print("=" * 110)

    for engine, passed in totals.items():
        print(
            f"{engine.capitalize():<8}: "
            f"{passed}/10 equivalentes"
        )

    print()

    if not failures and all(
        value == 10
        for value in totals.values()
    ):
        print(
            "[OK] LOS CUATRO MOTORES "
            "PRODUCEN RESULTADOS "
            "EQUIVALENTES Y RESPETAN "
            "EL ORDEN CONTRACTUAL"
        )

        raise SystemExit(0)

    print(
        "[ERROR] EXISTEN DIFERENCIAS "
        "O INCUMPLIMIENTOS DEL CONTRATO"
    )

    print()
    print(
        f"Incidencias: "
        f"{len(failures)}"
    )

    raise SystemExit(1)


if __name__ == "__main__":
    main()
