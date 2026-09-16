from pathlib import Path
import sys
import time

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DATASET_PATH = Path("data/processed/videogames_clean.parquet")


def main() -> None:
    print("=" * 100)
    print("DASK - PRUEBA DE ENTORNO")
    print("=" * 100)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro: {DATASET_PATH.resolve()}"
        )

    print(f"\nDask version: {dask.__version__}")
    print(f"Dataset: {DATASET_PATH.resolve()}")

    print("\n[1/4] Iniciando cluster local...")

    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        processes=True,
        memory_limit="2GB",
        dashboard_address=None,
    )

    client = Client(cluster)

    try:
        print("[OK] Cluster iniciado")
        print(f"Workers: {len(client.scheduler_info()['workers'])}")

        print("\n[2/4] Leyendo Parquet...")

        df = dd.read_parquet(
            DATASET_PATH,
            engine="pyarrow",
            split_row_groups="adaptive",
            blocksize="128MiB",
        )

        print(f"Particiones Dask: {df.npartitions}")
        print(f"Columnas: {len(df.columns)}")

        print("\n[3/4] Calculando registros...")

        start = time.perf_counter()

        row_count = df.map_partitions(len).sum().compute()

        elapsed_rows = time.perf_counter() - start

        print(f"Filas: {row_count:,}")
        print(
            f"Tiempo conteo filas: "
            f"{elapsed_rows:.4f} segundos"
        )

        print("\n[4/4] Validando IDs unicos...")

        start = time.perf_counter()

        unique_ids = df["id"].nunique().compute()

        elapsed_unique = time.perf_counter() - start

        print(f"IDs unicos: {unique_ids:,}")
        print(
            f"Tiempo IDs unicos: "
            f"{elapsed_unique:.4f} segundos"
        )

        print("\nPrimeras 5 filas:")

        print(
            df[
                [
                    "id",
                    "name",
                    "release_year",
                    "genres",
                    "platforms",
                ]
            ]
            .head(5)
        )

        print("\n" + "=" * 100)

        if (
            row_count == 899_585
            and unique_ids == 899_585
        ):
            print("[OK] DASK FUNCIONA CORRECTAMENTE")
        else:
            print("[ERROR] LOS RESULTADOS NO COINCIDEN")

        print("=" * 100)

    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
