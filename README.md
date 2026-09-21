# RAWG Big Data Project

Proyecto académico de Big Data construido sobre el dataset **RAWG Video Games Dataset**.

El objetivo es procesar un dataset de gran volumen utilizando diferentes tecnologías de procesamiento de datos y una arquitectura basada en Google Cloud Platform.

El proyecto compara cuatro motores:

- Polars
- Dask
- Modin + Ray
- Apache Spark

También incluye procesamiento Hadoop MapReduce utilizando:

- Google Cloud Dataproc
- HDFS
- `hadoop-mapreduce-examples.jar`

---

# 1. Resumen del proyecto

Dataset utilizado:

```text
videogames_data.csv
```

Características originales:

```text
Filas:     899,585
Columnas:  54
Tamaño:    ~1.9 GB
```

Después del proceso de limpieza se genera:

```text
data/processed/videogames_clean.parquet
```

Dataset procesado:

```text
Filas:     899,585
Columnas:  60
Tamaño:    ~591 MB
```

La conversión a Parquet permite reducir considerablemente el tamaño respecto al CSV original y facilita el procesamiento analítico.

---

# 2. Principales resultados

Validaciones realizadas sobre el dataset procesado:

```text
Registros:                 899,585
IDs únicos:                899,585
IDs duplicados:            0
Año mínimo:                1954
Año máximo:                2033
Juegos con rating:         18,676
Juegos con Metacritic:      7,130
Juegos con género:        685,982
Juegos con plataforma:    895,142
```

Algunos resultados obtenidos:

```text
Género con más juegos:
Action                     191,807

Plataforma con más juegos:
PC                         561,271

Desarrollador con más juegos:
SEGA                       585

Juego con mayor ratings_count:
Grand Theft Auto V         7,409
```

Los cuatro motores produjeron resultados equivalentes:

```text
Polars   10/10
Dask     10/10
Spark    10/10
Modin    10/10
```

---

# 3. Arquitectura

Arquitectura general utilizada:

```text
Kaggle / RAWG Dataset
        |
        v
videogames_data.csv
        |
        | limpieza y validación
        v
videogames_clean.parquet
        |
        v
Google Cloud Storage
        |
        +--------------------+
        |                    |
        v                    v
    Dataproc              Spark
        |
        v
      HDFS
        |
        v
 Hadoop MapReduce
```

Bucket utilizado durante el desarrollo:

```text
gs://rawg-bigdata-86233853262
```

Estructura principal:

```text
raw/
    videogames_data.csv

processed/
    videogames_clean.parquet

results/
    spark/
```

---

# 4. Estructura del repositorio

```text
GCLOUD_PROY
│
├── data
│   ├── raw
│   └── processed
│
├── docs
│
├── results
│   ├── polars
│   ├── dask
│   ├── modin
│   └── spark
│
└── src
    ├── common
    │   ├── inspect_dataset.py
    │   ├── profile_quality.py
    │   ├── inspect_values.py
    │   ├── clean_dataset.py
    │   ├── validate_clean_dataset.py
    │   ├── compare_polars_dask.py
    │   ├── compare_polars_dask_spark.py
    │   ├── compare_all_engines.py
    │   ├── compare_timings.py
    │   ├── copy_gcs_to_hdfs.sh
    │   ├── run_mapreduce_examples.sh
    │   ├── run_mapreduce_final.sh
    │   └── summarize_mapreduce.sh
    │
    ├── polars
    │   └── run_queries.py
    │
    ├── dask
    │   ├── smoke_test.py
    │   └── run_queries.py
    │
    ├── modin
    │   ├── smoke_test.py
    │   └── run_queries.py
    │
    └── spark
        ├── smoke_test.py
        └── run_queries.py
```

---

# 5. Importante sobre los datos

Los datasets no se almacenan en Git porque son demasiado grandes.

El `.gitignore` excluye:

```text
data/raw/*
data/processed/*
```

Por lo tanto, para ejecutar el proyecto se debe tener:

```text
data/raw/videogames_data.csv
```

o como mínimo el dataset procesado:

```text
data/processed/videogames_clean.parquet
```

---

# 6. Entorno principal

El desarrollo se realizó principalmente con:

```text
Python      3.11
Polars
Dask
PyArrow
pandas
```

Crear un entorno:

```powershell
conda create -n bigdata_rawg python=3.11 -y
conda activate bigdata_rawg
```

Instalar dependencias:

```powershell
python -m pip install `
    polars `
    pyarrow `
    pandas `
    "dask[distributed]"
```

---

# 7. Inspección y limpieza del dataset

Inspección inicial:

```powershell
python .\src\common\inspect_dataset.py
```

Perfil de calidad:

```powershell
python .\src\common\profile_quality.py
```

Inspección de valores:

```powershell
python .\src\common\inspect_values.py
```

Generar el Parquet limpio:

```powershell
python .\src\common\clean_dataset.py
```

Validar el resultado:

```powershell
python .\src\common\validate_clean_dataset.py
```

Resultado esperado:

```text
[OK] DATASET PROCESADO VALIDADO CORRECTAMENTE
```

---

# 8. Consultas oficiales Q01-Q10

Todos los motores ejecutan el mismo contrato de consultas.

Esto permite comparar los resultados de forma justa.

## Q01 - Resumen general

Calcula:

- total de registros;
- IDs únicos;
- año mínimo;
- año máximo;
- cantidad de juegos con rating;
- cantidad de juegos con Metacritic;
- cantidad de juegos con género;
- cantidad de juegos con plataforma.

Resultado base:

```text
total_registros          899585
ids_unicos               899585
anio_minimo              1954
anio_maximo              2033
juegos_con_rating        18676
juegos_con_metacritic    7130
juegos_con_genero        685982
juegos_con_plataforma    895142
```

## Q02 - Videojuegos por año

Agrupa los videojuegos por:

```text
release_year
```

y calcula el número de IDs únicos publicados en cada año.

---

## Q03 - Top géneros

La columna:

```text
genres
```

puede contener múltiples géneros separados por:

```text
|
```

La consulta:

1. separa los géneros;
2. genera una fila por género;
3. cuenta juegos únicos;
4. ordena de mayor a menor.

Ejemplo:

```text
Action        191807
Adventure     152129
Platformer    100949
```

---

## Q04 - Top plataformas

Realiza el mismo proceso utilizando:

```text
platforms
```

Ejemplo:

```text
PC       561271
Web      260067
macOS    108113
```

---

## Q05 - Top desarrolladores

Separa:

```text
developers
```

y cuenta el número de juegos únicos por desarrollador.

Ejemplo:

```text
SEGA                              585
Sony Interactive Entertainment   527
Konami Digital Entertainment     485
Capcom                            480
```

---

## Q06 - Rating por género

Por cada género calcula:

- cantidad de juegos con rating;
- rating promedio;
- rating mediano.

Solo se consideran géneros con al menos:

```text
50 juegos con rating
```

---

## Q07 - Metacritic por género

Por cada género calcula:

- cantidad de juegos con Metacritic;
- promedio de Metacritic;
- mediana de Metacritic.

Solo se consideran géneros con al menos:

```text
20 observaciones
```

---

## Q08 - Juegos con mayor número de ratings

Ordena por:

```text
ratings_count
```

y obtiene los 20 primeros videojuegos.

Las columnas mostradas son:

```text
id
name
release_year
rating
ratings_count
reviews_count
genres
```

---

## Q09 - Playtime por género

Por género calcula:

- juegos con playtime;
- playtime promedio;
- mediana;
- máximo.

Se consideran géneros con al menos:

```text
30 observaciones
```

---

## Q10 - Evolución anual

Por cada año calcula:

- número de videojuegos;
- juegos con rating;
- rating promedio;
- juegos con Metacritic;
- Metacritic promedio;
- total de ratings.

Esta consulta permite estudiar la evolución temporal del dataset.

---

# 9. Ejecutar Polars

Activar:

```powershell
conda activate bigdata_rawg
```

Ejecutar:

```powershell
python .\src\polars\run_queries.py |
    Tee-Object `
        -FilePath .\results\polars_execution.txt
```

Los resultados quedan en:

```text
results/polars/
```

Tiempo observado en la revisión final:

```text
1.0032 segundos
```

para Q01-Q10.

---

# 10. Ejecutar Dask

Primero se puede validar el clúster local:

```powershell
python .\src\dask\smoke_test.py
```

Durante el desarrollo se utilizó:

```text
4 workers
11 particiones
899,585 registros
```

Ejecutar las consultas:

```powershell
python .\src\dask\run_queries.py |
    Tee-Object `
        -FilePath .\results\dask_execution.txt
```

Resultados:

```text
results/dask/
```

Tiempo observado en la revisión final:

```text
24.1221 segundos
```

---

# 11. Entorno Modin + Ray

Modin utiliza una versión de pandas distinta al entorno principal.

Por eso se recomienda un entorno separado.

Crear:

```powershell
conda create `
    -n bigdata_modin `
    python=3.11 `
    -y

conda activate bigdata_modin
```

Instalar:

```powershell
python -m pip install `
    "modin[ray]==0.37.1" `
    "pyarrow==25.0.1"
```

Versiones utilizadas durante el desarrollo:

```text
Python     3.11.16
Modin      0.37.1
Ray        2.58.0
pandas     2.3.3
PyArrow    25.0.1
```

---

# 12. Ejecutar Modin

Smoke test:

```powershell
python .\src\modin\smoke_test.py
```

Resultado esperado:

```text
[OK] MODIN + RAY LEE CORRECTAMENTE EL DATASET PROCESADO
```

Ejecutar Q01-Q10:

```powershell
python .\src\modin\run_queries.py |
    Tee-Object `
        -FilePath .\results\modin_execution.txt
```

Resultados:

```text
results/modin/
```

Tiempo observado en la revisión final:

```text
Carga dataset:
80.2687 s

Q01-Q10:
83.7464 s

Carga + consultas:
164.0151 s
```

---

# 13. Spark en Google Dataproc

Spark fue ejecutado realmente en Google Cloud Dataproc.

Configuración utilizada:

```text
Región:       us-central1
Zona:         us-central1-b

Master:       1
Workers:      2

Máquina:
e2-standard-2
```

Antes de continuar se necesita:

```powershell
gcloud auth login
```

Configurar proyecto:

```powershell
gcloud config set project proyecto-yvette
```

Variables:

```powershell
$PROJECT_ID = "proyecto-yvette"
$REGION = "us-central1"
$ZONE = "us-central1-b"
$CLUSTER = "rawg-bigdata-cluster"
```

---

# 14. Crear clúster Dataproc

```powershell
gcloud dataproc clusters create $CLUSTER `
    --project=$PROJECT_ID `
    --region=$REGION `
    --zone=$ZONE `
    --image-version=2.2.87-debian12 `
    --master-machine-type=e2-standard-2 `
    --worker-machine-type=e2-standard-2 `
    --num-workers=2 `
    --master-boot-disk-type=pd-standard `
    --worker-boot-disk-type=pd-standard `
    --master-boot-disk-size=50GB `
    --worker-boot-disk-size=50GB `
    --enable-component-gateway `
    --delete-max-age=12h `
    "--labels=project=rawg,course=bigdata"
```

Verificar:

```powershell
gcloud dataproc clusters list `
    --project=$PROJECT_ID `
    --region=$REGION
```

Debe aparecer:

```text
rawg-bigdata-cluster   RUNNING
```

---

# 15. Spark smoke test

```powershell
gcloud dataproc jobs submit pyspark `
    .\src\spark\smoke_test.py `
    --project=$PROJECT_ID `
    --region=$REGION `
    --cluster=$CLUSTER |
    Tee-Object `
        -FilePath .\results\spark_smoke_test.txt
```

Resultado esperado:

```text
Filas       : 899,585
IDs unicos  : 899,585
Anio minimo : 1954
Anio maximo : 2033

[OK] SPARK LEE CORRECTAMENTE EL PARQUET DESDE GCS
```

---

# 16. Ejecutar Spark Q01-Q10

```powershell
gcloud dataproc jobs submit pyspark `
    .\src\spark\run_queries.py `
    --project=$PROJECT_ID `
    --region=$REGION `
    --cluster=$CLUSTER |
    Tee-Object `
        -FilePath .\results\spark_execution_final.txt
```

Los resultados se escriben en:

```text
gs://rawg-bigdata-86233853262/results/spark/
```

Durante la ejecución revisada final en Dataproc:

```text
Tiempo total Q01-Q10:
118.0391 segundos
```

---

# 17. Descargar resultados Spark

Para poder compararlos localmente:

```powershell
$BUCKET = "rawg-bigdata-86233853262"

New-Item `
    -ItemType Directory `
    -Force `
    .\results\spark |
    Out-Null

$queries = @{
    "q01_resumen_general"          = "q01_resumen_general.csv"
    "q02_videojuegos_por_anio"     = "q02_videojuegos_por_anio.csv"
    "q03_top_generos"              = "q03_top_generos.csv"
    "q04_top_plataformas"          = "q04_top_plataformas.csv"
    "q05_top_desarrolladores"      = "q05_top_desarrolladores.csv"
    "q06_rating_por_genero"        = "q06_rating_por_genero.csv"
    "q07_metacritic_por_genero"    = "q07_metacritic_por_genero.csv"
    "q08_top_videojuegos_ratings"  = "q08_top_videojuegos_ratings.csv"
    "q09_playtime_por_genero"      = "q09_playtime_por_genero.csv"
    "q10_evolucion_anual"          = "q10_evolucion_anual.csv"
}

foreach ($query in $queries.Keys) {

    $part = gcloud storage ls `
        "gs://$BUCKET/results/spark/$query/part-*.csv" |
        Select-Object -First 1

    if (-not $part) {
        throw "No se encontró CSV para $query"
    }

    gcloud storage cp `
        $part `
        ".\results\spark\$($queries[$query])"
}
```

---

# 18. Comparar los cuatro motores

La validación funcional final está en:

```text
src/common/compare_all_engines.py
```

Ejecutar:

```powershell
conda activate bigdata_rawg
```

Luego:

```powershell
python .\src\common\compare_all_engines.py |
    Tee-Object `
        -FilePath .\results\compare_all_engines.txt
```

El programa utiliza a Polars como referencia y compara:

```text
Polars vs Dask
Polars vs Spark
Polars vs Modin
```

para las diez consultas.

La comparación tolera diferencias numéricas de:

```text
0.0001
```

Esto es necesario por pequeñas diferencias de representación y redondeo entre motores.

Resultado esperado:

```text
Dask  equivalentes : 10/10
Dask  diferentes   : 0/10

Spark equivalentes : 10/10
Spark diferentes   : 0/10

Modin equivalentes : 10/10
Modin diferentes   : 0/10

[OK] LOS CUATRO MOTORES PRODUCEN RESULTADOS EQUIVALENTES
```

---

# 19. Diferencias de redondeo

Durante la validación se encontró un caso interesante.

Para Metacritic de 2004 el valor matemático era:

```text
72.15625
```

Spark utilizando `round()` obtenía:

```text
72.1563
```

mientras Polars y Dask obtenían:

```text
72.1562
```

Para estandarizar la política se utilizó:

```python
F.bround(...)
```

en Spark.

Después del cambio:

```text
Polars  72.1562
Dask    72.1562
Spark   72.1562
Modin   72.1562
```

y las 10 consultas quedaron equivalentes.

---

# 20. Comparación de tiempos

El programa:

```text
src/common/compare_timings.py
```

busca los archivos de ejecución y los CSV de tiempos disponibles.

Ejecutar:

```powershell
python .\src\common\compare_timings.py
```

El resultado se guarda en:

```text
results/timing_comparison.csv
```

Resultados observados en la revisión final (Q01-Q10):

| Motor | Entorno | Tiempo Q01-Q10 |
|---|---|---:|
| Polars | Local (Lazy API) | 1.0032 s |
| Dask | Local / 4 workers, 11 particiones | 24.1221 s |
| Modin + Ray | Local / entorno separado (consultas puras) | 83.7464 s |
| Spark | Dataproc / 1 master + 2 workers | 118.0391 s |

## Advertencia

Estos tiempos NO constituyen un benchmark hardware-a-hardware.

Polars, Dask y Modin fueron ejecutados localmente.

Spark fue ejecutado en Google Dataproc.

Por lo tanto, la comparación debe analizar también:

- overhead de inicialización;
- arquitectura;
- paralelismo;
- escalabilidad;
- distribución;
- infraestructura utilizada.

No debe concluirse únicamente que un motor es mejor que otro por el tiempo total.

---

# 21. Hadoop y HDFS

El dataset RAW también fue procesado con Hadoop.

Archivo:

```text
/raw/videogames_data.csv
```

Tamaño:

```text
2,000,469,667 bytes
```

Configuración observada:

```text
Hadoop:       3.3.6
Spark:        3.5.3
HDFS block:   128 MiB
Replication:  2
```

El archivo produjo:

```text
15 input splits
```

---

# 22. Programas MapReduce

Se ejecutaron tres programas de:

```text
/usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar
```

Programas:

```text
wordmean
wordmedian
wordstandarddeviation
```

No se utilizaron:

```text
wordcount
grep
```

---

# 23. Resultados Hadoop

Número de tokens:

```text
203,389,383
```

Suma de longitudes:

```text
1,771,224,192
```

Media:

```text
8.708538104961
```

Mediana:

```text
4
```

Desviación estándar calculada:

```text
22.739068817670
```

El ejemplo oficial `wordstandarddeviation` produjo `NaN` con este volumen.

El job MapReduce sí finalizó correctamente.

El problema se estudió posteriormente utilizando las frecuencias generadas por Hadoop para consolidar la estadística correctamente.

---

# 24. Cerrar el clúster

Dataproc genera costo mientras el clúster está activo.

Después de terminar Spark/Hadoop:

```powershell
gcloud dataproc clusters delete `
    rawg-bigdata-cluster `
    --project=proyecto-yvette `
    --region=us-central1 `
    --quiet
```

Verificar:

```powershell
gcloud dataproc clusters list `
    --project=proyecto-yvette `
    --region=us-central1
```

Resultado esperado:

```text
Listed 0 items.
```

Cloud Storage NO se elimina al borrar el clúster.

---

# 25. Ejecución rápida para validación local

Si solamente se quiere comprobar el proyecto y ya se dispone del Parquet:

## Polars

```powershell
conda activate bigdata_rawg

python .\src\polars\run_queries.py
```

## Dask

```powershell
python .\src\dask\smoke_test.py
python .\src\dask\run_queries.py
```

## Modin

```powershell
conda activate bigdata_modin

python .\src\modin\smoke_test.py
python .\src\modin\run_queries.py
```

## Comparar todos

```powershell
conda activate bigdata_rawg

python .\src\common\compare_all_engines.py
```

Para este último paso deben existir previamente los resultados de:

```text
results/polars/
results/dask/
results/spark/
results/modin/
```

---

# 26. Orden recomendado para reproducir todo

```text
1. Obtener videogames_data.csv
2. Crear entorno bigdata_rawg
3. Ejecutar profile/inspect
4. Ejecutar clean_dataset.py
5. Ejecutar validate_clean_dataset.py

6. Ejecutar Polars Q01-Q10
7. Ejecutar Dask Q01-Q10

8. Crear entorno bigdata_modin
9. Ejecutar Modin Q01-Q10

10. Configurar Google Cloud
11. Subir Parquet a GCS
12. Crear Dataproc
13. Ejecutar Spark Q01-Q10
14. Descargar resultados Spark

15. Ejecutar compare_all_engines.py
16. Ejecutar compare_timings.py

17. Ejecutar Hadoop/HDFS si se desea reproducir
    completamente la evaluación cloud

18. Eliminar el clúster Dataproc
```

---

# 27. Resultado final esperado

La validación final debe terminar con:

```text
[OK] POLARS 10/10
[OK] DASK   10/10
[OK] SPARK  10/10
[OK] MODIN  10/10

[OK] LOS CUATRO MOTORES PRODUCEN RESULTADOS EQUIVALENTES
```

Esto confirma que las diferentes implementaciones realizan las mismas operaciones analíticas sobre el mismo dataset.

## Validacion final de revision

El 18 de septiembre de 2026 se realizo una revision adicional del pipeline RAWG para reforzar reproducibilidad, validacion y consistencia entre motores.

### Mejoras aplicadas

- Los valores `NaN` numericos se convierten a `NULL` durante la limpieza.
- Los valores literales `NULL`, `None` y `NaN` en `name` y `slug` se preservan como texto.
- Las dimensiones multivalor se normalizan eliminando espacios, elementos vacios y valores repetidos.
- Los conteos derivados de generos, plataformas, desarrolladores y publishers se calculan sobre la dimension normalizada.
- El validador comprueba explicitamente la coherencia `released` / `release_year`.
- Se validan los rangos de `rating`, `metacritic` y `playtime`.
- Los rankings usan criterios de desempate deterministas.
- Las parejas `id-dimension` repetidas no contribuyen mas de una vez a las estadisticas.
- El comparador preserva textos literales, aplica tolerancia `0.0001` solo a valores numericos y valida tambien el orden contractual.
- El smoke test de Dask devuelve codigo de error cuando una comprobacion falla.
- El script Hadoop evita el riesgo de `SIGPIPE` provocado por `head` bajo `pipefail`.

### Resultado final

El dataset procesado conserva:

- 899,585 registros.
- 60 columnas.
- 899,585 IDs unicos.
- 0 problemas estructurales en la auditoria final.
- 0 inconsistencias en las dimensiones derivadas.
- 0 valores de `rating` fuera del rango permitido.
- 0 inconsistencias entre `released` y `release_year`.

Las diez consultas oficiales se volvieron a validar con Polars, Dask, Modin y Spark.

Resultado del comparador final:

- Dask: 10/10 equivalentes.
- Spark: 10/10 equivalentes.
- Modin: 10/10 equivalentes.
- Orden contractual: correcto.
- Tolerancia numerica: 0.0001.

La ejecucion final de Spark se realizo en Google Dataproc mediante el job:

`9f9d25248f214fae8164c6d8d7c1d837`

El job finalizo con estado `DONE`.

El Parquet revisado tiene SHA256 local:

`32D1854BD378ECF67687D3A917264893D031AE701D499BDB298255F7D87C142E`

El archivo subido a Google Cloud Storage fue verificado mediante CRC32C antes de la ejecucion de Spark.

BigQuery no forma parte del pipeline RAWG y no se modificaron recursos de BigQuery durante este proyecto.
