# Contrato oficial de consultas

Las siguientes consultas deben implementarse con la misma lógica en:

- Polars
- Dask
- Modin
- Apache Spark

## Q01 - Resumen general
Calcular:
- total de registros
- IDs únicos
- año mínimo
- año máximo
- juegos con rating
- juegos con Metacritic
- juegos con género
- juegos con plataforma

## Q02 - Videojuegos lanzados por año
Excluir release_year NULL.
Agrupar por release_year.
Calcular cantidad de videojuegos.
Ordenar cronológicamente.

## Q03 - Top géneros por cantidad de videojuegos
Excluir genres NULL.
Separar genres por "|".
Expandir cada género.
Agrupar por género.
Contar videojuegos únicos.
Ordenar descendente.
Mostrar Top 20.

## Q04 - Top plataformas por cantidad de videojuegos
Excluir platforms NULL.
Separar platforms por "|".
Expandir cada plataforma.
Agrupar por plataforma.
Contar videojuegos únicos.
Ordenar descendente.
Mostrar Top 20.

## Q05 - Top desarrolladores por cantidad de videojuegos
Excluir developers NULL.
Separar developers por "|".
Expandir cada desarrollador.
Agrupar por desarrollador.
Contar videojuegos únicos.
Ordenar descendente.
Mostrar Top 20.

## Q06 - Rating promedio por género
Usar solamente filas con rating y genres no NULL.
Separar y expandir genres.
Agrupar por género.
Calcular:
- videojuegos con rating
- rating promedio
- rating mediano

Excluir géneros con menos de 50 videojuegos con rating.
Ordenar por rating promedio descendente.
Mostrar Top 20.

## Q07 - Metacritic promedio por género
Usar solamente filas con metacritic y genres no NULL.
Separar y expandir genres.
Agrupar por género.
Calcular:
- videojuegos con Metacritic
- Metacritic promedio
- Metacritic mediano

Excluir géneros con menos de 20 videojuegos con Metacritic.
Ordenar por promedio descendente.
Mostrar Top 20.

## Q08 - Videojuegos con mayor cantidad de ratings
Excluir ratings_count NULL.
Ordenar por ratings_count descendente.
Mostrar Top 20 con:
- id
- name
- release_year
- rating
- ratings_count
- reviews_count
- genres

## Q09 - Playtime promedio por género
Usar solamente filas con playtime y genres no NULL.
Separar y expandir genres.
Agrupar por género.
Calcular:
- juegos con playtime
- playtime promedio
- playtime mediano
- playtime máximo

Excluir géneros con menos de 30 juegos con playtime.
Ordenar por playtime promedio descendente.
Mostrar Top 20.

## Q10 - Evolución anual
Excluir release_year NULL.
Agrupar por año.
Calcular:
- cantidad de videojuegos
- juegos con rating
- rating promedio
- juegos con Metacritic
- Metacritic promedio
- total de ratings registrados

Ordenar cronológicamente.
