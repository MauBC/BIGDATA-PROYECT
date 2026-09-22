# CRUD de registros RAWG



## Objetivo

Demostrar las operaciones de crear, consultar, actualizar y eliminar

registros del dataset RAWG sobre una copia de trabajo.



## Entorno

Python 3.12.3 y Polars 1.44.2, ejecutados en Google Cloud Shell.

Proyecto: proyecto-yvette.

Esta demostracion no es una ejecucion distribuida en Dataproc.



## Datos

Se utilizaron las primeras 1000 filas del Parquet limpio,

conservando las 60 columnas y sus tipos.



Fuente:

gs://rawg-bigdata-86233853262/processed/videogames_clean.parquet



## Resultados

- Inicial: 1000 filas; el ID 1018371 no existia.

- Create: se agrego el videojuego sintetico; quedaron 1001 filas.

- Read: se encontro exactamente un registro con ese ID y rating 3.0.

- Update: su rating cambio de 3.0 a 4.5; se mantuvieron 1001 filas.

- Delete: se elimino el registro sintetico; quedaron 1000 filas.



El registro sintetico contiene id, name, slug y rating.

Los demas campos son nulos. Su ID se obtuvo sumando uno al

maximo ID del archivo completo para evitar colisiones.



## Validaciones

Se comprobo la conservacion del esquema y la lectura de los

Parquet guardados. Update no altero los registros originales.

La muestra final fue identica a la inicial y el SHA-256 del

archivo de entrada no cambio.



## Archivos

Script:

src/polars/crud_rawg.py



Evidencias en el repositorio:

results/crud/20260922T001035Z_7612d7fc/



Respaldo completo con los Parquet y el script:

gs://rawg-bigdata-86233853262/results/crud_leonardo/20260922T001035Z_7612d7fc/



## Reproduccion

Con Polars 1.44.2 instalado, desde la raiz del repositorio:



python src/polars/crud_rawg.py --input /ruta/videogames_clean.parquet --output results/crud



Cada ejecucion crea una carpeta nueva. El script no modifica

el archivo de entrada. Las evidencias se suben al bucket por separado.
