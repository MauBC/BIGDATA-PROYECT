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

---

## Política determinista de ordenamiento

Para garantizar resultados reproducibles entre Polars, Dask, Modin y Spark,
todas las consultas deben utilizar un criterio de desempate explícito.

### Q02 - Videojuegos por año

Orden:

1. `release_year` ascendente.

### Q03 - Top géneros

Orden:

1. `videojuegos` descendente.
2. `genres` ascendente.

### Q04 - Top plataformas

Orden:

1. `videojuegos` descendente.
2. `platforms` ascendente.

### Q05 - Top desarrolladores

Orden:

1. `videojuegos` descendente.
2. `developers` ascendente.

### Q06 - Rating por género

Orden:

1. `rating_promedio` descendente.
2. `genres` ascendente.

### Q07 - Metacritic por género

Orden:

1. `metacritic_promedio` descendente.
2. `genres` ascendente.

### Q08 - Top videojuegos por cantidad de ratings

Orden:

1. `ratings_count` descendente.
2. `id` ascendente.

### Q09 - Playtime por género

Orden:

1. `playtime_promedio` descendente.
2. `genres` ascendente.

### Q10 - Evolución anual

Orden:

1. `release_year` ascendente.

## Unidad de observación para dimensiones

Para estadísticas por dimensiones como géneros, plataformas,
desarrolladores o publishers:

- los valores se separan por `|`;
- se eliminan espacios laterales;
- se eliminan elementos vacíos;
- una pareja `id-dimensión` no debe aparecer más de una vez.

Por ejemplo:

`Action||RPG|`

equivale a:

`Action|RPG`

y:

`Action|Action`

debe hacer que ese videojuego contribuya una sola vez a `Action`.

## Comparación entre motores

La comparación debe cumplir simultáneamente:

1. mismas columnas;
2. mismo número de filas;
3. mismos valores;
4. mismo orden;
5. tolerancia absoluta máxima `0.0001` únicamente para columnas numéricas.

Los textos literales `NULL`, `None` y `NaN` deben conservarse como texto
cuando realmente forman parte del dato y no deben convertirse
automáticamente en valores faltantes durante la comparación.
