from __future__ import annotations

import csv
import re
from pathlib import Path


RESULTS = Path("results")


ENGINES = {
    "Polars": {
        "logs": [
            RESULTS / "polars_execution.txt",
            RESULTS / "polars" / "execution.txt",
        ],
        "dir": RESULTS / "polars",
        "environment": "Laptop local",
    },

    "Dask": {
        "logs": [
            RESULTS / "dask_execution.txt",
            RESULTS / "dask" / "execution.txt",
        ],
        "dir": RESULTS / "dask",
        "environment": "Laptop local - Dask distribuido",
    },

    "Modin": {
        "logs": [
            RESULTS / "modin_execution.txt",
        ],
        "dir": RESULTS / "modin",
        "environment": "Laptop local - Modin + Ray",
    },

    "Spark": {
        "logs": [
            RESULTS / "spark_execution_final.txt",
            RESULTS / "spark_execution.txt",
        ],
        "dir": RESULTS / "spark",
        "environment": "Google Dataproc - Spark",
    },
}


TIME_PATTERNS = [
    re.compile(
        r"Tiempo\s+total\s+consultas\s*:\s*"
        r"([0-9]+(?:[.,][0-9]+)?)",
        re.IGNORECASE,
    ),

    re.compile(
        r"Tiempo\s+total(?:\s+\w+)*\s*:\s*"
        r"([0-9]+(?:[.,][0-9]+)?)",
        re.IGNORECASE,
    ),
]


def read_text_robust(path: Path) -> str:
    raw = path.read_bytes()

    # BOM UTF-16 LE
    if raw.startswith(b"\xff\xfe"):
        return raw.decode(
            "utf-16-le",
            errors="ignore",
        )

    # BOM UTF-16 BE
    if raw.startswith(b"\xfe\xff"):
        return raw.decode(
            "utf-16-be",
            errors="ignore",
        )

    # BOM UTF-8
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode(
            "utf-8-sig",
            errors="ignore",
        )

    # Muchos NUL suelen indicar UTF-16 sin BOM.
    if raw[:2000].count(b"\x00") > 20:

        try:
            return raw.decode(
                "utf-16-le",
                errors="ignore",
            )
        except UnicodeError:
            pass

    for encoding in [
        "utf-8",
        "cp1252",
        "latin-1",
    ]:

        try:
            return raw.decode(encoding)

        except UnicodeDecodeError:
            continue

    return raw.decode(
        "utf-8",
        errors="ignore",
    )


def parse_log(path: Path):

    if not path.exists():
        return None

    text = read_text_robust(path)

    values = []

    for pattern in TIME_PATTERNS:

        matches = pattern.findall(text)

        for value in matches:

            values.append(
                float(
                    value.replace(",", ".")
                )
            )

    if not values:
        return None

    return values[-1]


def parse_timing_csv(directory: Path):

    if not directory.exists():
        return None

    candidates = list(
        directory.glob("*tiempo*.csv")
    )

    candidates += list(
        directory.glob("*timing*.csv")
    )

    for path in candidates:

        try:

            with path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as file:

                reader = csv.DictReader(file)

                rows = list(reader)

                if not rows:
                    continue

                possible_columns = [
                    "tiempo_segundos",
                    "duration_seconds",
                    "seconds",
                    "time_seconds",
                ]

                for column in possible_columns:

                    if column not in rows[0]:
                        continue

                    total = 0.0

                    for row in rows:

                        value = row.get(column)

                        if value in (
                            None,
                            "",
                        ):
                            continue

                        total += float(
                            value.replace(",", ".")
                        )

                    return total

        except (
            ValueError,
            OSError,
            csv.Error,
        ):
            continue

    return None


def get_engine_time(
    config,
):

    for path in config["logs"]:

        value = parse_log(path)

        if value is not None:

            return (
                value,
                str(path),
            )

    value = parse_timing_csv(
        config["dir"]
    )

    if value is not None:

        return (
            value,
            str(config["dir"]),
        )

    return (
        None,
        None,
    )


def main():

    print("=" * 105)

    print(
        "COMPARACION DE TIEMPOS - "
        "POLARS / DASK / MODIN / SPARK"
    )

    print("=" * 105)

    results = []

    for engine, config in ENGINES.items():

        seconds, source = get_engine_time(
            config
        )

        results.append(
            {
                "motor": engine,
                "entorno": config[
                    "environment"
                ],
                "tiempo_segundos": seconds,
                "fuente": source,
            }
        )

    available = [
        row
        for row in results
        if row["tiempo_segundos"]
        is not None
    ]

    fastest = None

    if available:

        fastest = min(
            row["tiempo_segundos"]
            for row in available
        )

    print()

    print(
        f"{'Motor':<10}"
        f"{'Tiempo (s)':>15}  "
        f"{'vs mas rapido':>15}  "
        f"Entorno"
    )

    print("-" * 105)

    for row in results:

        seconds = row[
            "tiempo_segundos"
        ]

        if seconds is None:

            print(
                f"{row['motor']:<10}"
                f"{'SIN DATO':>15}  "
                f"{'-':>15}  "
                f"{row['entorno']}"
            )

            continue

        ratio = seconds / fastest

        print(
            f"{row['motor']:<10}"
            f"{seconds:>15.4f}  "
            f"{ratio:>14.2f}x  "
            f"{row['entorno']}"
        )

    output = (
        RESULTS
        / "timing_comparison.csv"
    )

    with output.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "motor",
                "entorno",
                "tiempo_segundos",
                "fuente",
            ],
        )

        writer.writeheader()

        writer.writerows(
            results
        )

    print()

    print(
        f"Resultado guardado en: "
        f"{output}"
    )

    print()

    print(
        "Fuentes detectadas:"
    )

    for row in results:

        print(
            f"  {row['motor']:<7}: "
            f"{row['fuente'] or 'NO ENCONTRADA'}"
        )

    print()

    print(
        "IMPORTANTE: estos tiempos son "
        "observados en entornos distintos."
    )

    print(
        "Spark corre en Google Dataproc, "
        "mientras Polars, Dask y Modin "
        "se ejecutan localmente."
    )

    print(
        "No deben interpretarse como un "
        "benchmark hardware-a-hardware directo."
    )


if __name__ == "__main__":
    main()
