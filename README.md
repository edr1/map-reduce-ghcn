# GHCN-Daily MapReduce

Proyecto Java/Hadoop para analizar los CSV de NOAA GHCN-Daily publicados en
`https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/`.

## Análisis implementados

1. `avg-tmax`: promedio de TMAX por estación y año. Mapper emite
   `(estación|año, suma=temperatura, conteo=1)`; Combiner agrega suma/conteo;
   Reducer calcula el promedio.
2. `monthly-prcp`: precipitación acumulada por estación y mes. Mapper emite
   `(estación|AAAA-MM, precipitación)`; Combiner y Reducer suman.
3. `annual-max`: mayor TMAX por año con estación y fecha. Mapper emite
   `(año, temperatura|estación|fecha)`; Combiner y Reducer conservan el máximo.

## Reglas de datos

- Se usa Apache Commons CSV: `NAME` puede contener comas.
- Se ignora la cabecera de cada archivo.
- Se descartan celdas vacías y marcadores formados por 9.
- En `*_ATTRIBUTES`, el segundo indicador es el Quality Flag; un valor no vacío
  descarta la observación.
- Los valores del archivo de ejemplo están en décimas: `276` se interpreta como
  `27.6 °C` y `15` como `1.5 mm`.

## Compilar

```bash
mvn clean package
```

El artefacto ejecutable queda en:

```text
target/ghcnd-mapreduce-1.0.0.jar
```

## Ejecutar en Amazon EMR

```bash
hadoop jar target/ghcnd-mapreduce-1.0.0.jar \
  avg-tmax s3://BUCKET/noaa-ghcn/input/ s3://BUCKET/noaa-ghcn/output/avg-tmax

hadoop jar target/ghcnd-mapreduce-1.0.0.jar \
  monthly-prcp s3://BUCKET/noaa-ghcn/input/ s3://BUCKET/noaa-ghcn/output/monthly-prcp

hadoop jar target/ghcnd-mapreduce-1.0.0.jar \
  annual-max s3://BUCKET/noaa-ghcn/input/ s3://BUCKET/noaa-ghcn/output/annual-max
```

Hadoop exige que el directorio de salida no exista. El script
`scripts/ejecutar_emr.sh` agrega una marca temporal para evitar colisiones.

## Inspección de resultados

```bash
aws s3 cp s3://BUCKET/noaa-ghcn/output/avg-tmax/part-r-00000 - | head
aws s3 cp s3://BUCKET/noaa-ghcn/output/monthly-prcp/part-r-00000 - | head
aws s3 cp s3://BUCKET/noaa-ghcn/output/annual-max/part-r-00000 - | head
```
