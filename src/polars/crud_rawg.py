"""CRUD de registros RAWG sobre una muestra local en Cloud Shell.

Uso: python crud_rawg.py --input videogames_clean.parquet
Requiere: polars==1.44.2
No escribe en GCS ni modifica el Parquet de entrada.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import polars as pl


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=Path("results/crud"))
    args = parser.parse_args()
    check(args.rows > 0, "--rows debe ser positivo")
    source = args.input.resolve(strict=True)
    original_hash = sha256(source)
    base = pl.read_parquet(source, n_rows=args.rows)
    check(base.height > 0, "El dataset está vacío")
    check(all(c in base.columns for c in ("id", "name", "slug", "rating")),
          "Faltan columnas necesarias: id, name, slug, rating")
    check(base.schema["id"] == pl.Int64, "Se esperaba id Int64")

    # Solo se consulta la columna id completa para evitar colisiones globales.
    max_id = pl.scan_parquet(source).select(pl.col("id").max()).collect().item()
    test_id = max(0, max_id if max_id is not None else 0) + 1
    check(test_id <= 2**63 - 1, "No hay espacio para un nuevo id Int64")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = args.output.resolve() / run_id
    output.mkdir(parents=True, exist_ok=False)
    log: list[str] = []
    summary: list[dict] = []
    visible = ["id", "name", "slug", "rating"]

    def snapshot(stage: str, frame: pl.DataFrame) -> None:
        check(frame.schema == base.schema, f"Cambió el esquema en {stage}")
        target = output / f"{stage}.parquet"
        frame.write_parquet(target, compression="zstd")
        check(pl.read_parquet(target).equals(frame), f"Falló persistencia de {stage}")
        record = frame.filter(pl.col("id") == test_id).select(visible)
        record.write_csv(output / f"{stage}_registro.csv")
        text = f"\n{stage} | filas: {frame.height} | coincidencias ID {test_id}: {record.height}\n{record}"
        print(text)
        log.append(text)
        summary.append({"etapa": stage, "filas": frame.height,
                        "id_prueba": test_id, "coincidencias": record.height})

    snapshot("00_antes", base)

    # CREATE: registro explícitamente sintético; los datos desconocidos son null.
    values = {column: None for column in base.columns}
    values.update(id=test_id, slug=f"crud-demo-{test_id}",
                  name="VIDEOJUEGO SINTETICO - DEMO CRUD", rating=3.0)
    new = pl.DataFrame([values], schema=base.schema)
    created = pl.concat([base, new])
    check(created.height == base.height + 1, "CREATE no añadió una fila")
    snapshot("01_create", created)

    # READ: consultar desde la copia persistida, no solo desde memoria.
    work = pl.read_parquet(output / "01_create.parquet")
    found = work.filter(pl.col("id") == test_id)
    check(found.height == 1 and found["rating"].item() == 3.0, "READ incorrecto")
    snapshot("02_read", work)

    # UPDATE: modificar únicamente el rating del registro de prueba.
    updated = work.with_columns(
        pl.when(pl.col("id") == test_id).then(pl.lit(4.5))
        .otherwise(pl.col("rating")).cast(base.schema["rating"]).alias("rating")
    )
    check(updated.filter(pl.col("id") == test_id)["rating"].item() == 4.5,
          "UPDATE no modificó el rating")
    others = updated.filter((pl.col("id") != test_id).fill_null(True))
    check(others.equals(base), "UPDATE alteró registros originales")
    snapshot("03_update", updated)

    # DELETE: quitar el registro de la copia; preservar posibles id nulos.
    work = pl.read_parquet(output / "03_update.parquet")
    deleted = work.filter((pl.col("id") != test_id).fill_null(True))
    check(deleted.equals(base), "DELETE no recuperó la muestra inicial exacta")
    snapshot("04_delete", deleted)

    final_hash = sha256(source)
    check(original_hash == final_hash, "El archivo de entrada fue modificado")
    pl.DataFrame(summary).write_csv(output / "resumen.csv")
    metadata = {"estado": "OK", "fecha_utc": run_id, "python": platform.python_version(),
                "polars": pl.__version__, "entrada_local": str(source),
                "filas_muestra": base.height, "columnas": base.width,
                "id_prueba": test_id, "sha256_antes": original_hash,
                "sha256_despues": final_hash, "entrada_sin_cambios": True,
                "muestra_final_igual_inicial": True,
                "alcance": "CRUD local con Polars; no es una ejecución distribuida",
                "registro_sintetico": "Solo id, slug, name y rating; resto null"}
    (output / "validacion.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    final = f"\nCRUD OK: 4 operaciones verificadas.\nEvidencias: {output}"
    print(final)
    (output / "ejecucion.txt").write_text("\n".join(log) + final + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
