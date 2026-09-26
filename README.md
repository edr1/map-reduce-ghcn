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

Los tres jobs usan `CombineTextInputFormat` para agrupar archivos pequeños.
Para limitar cada split a aproximadamente 128 MiB, coloca
`-Dmapreduce.input.fileinputformat.split.maxsize=134217728` antes del nombre
del análisis. Este parámetro no agrupa archivos si ejecutas un JAR antiguo
que todavía utiliza `TextInputFormat`.

Después de cambiar el código, recompila y copia el JAR de `target/` al nodo
Primary. Verifica que el comando ejecute esa copia actualizada. El log
`number of splits` debe reflejar la agrupación; el número de archivos de
entrada no cambia. Usa una ruta de salida nueva para cada ejecución.

```bash
hadoop jar target/ghcnd-mapreduce-1.0.0.jar avg-tmax s3://dataset-ghcn-2/input/ s3://dataset-ghcn-2/output/avg-tmax

hadoop jar target/ghcnd-mapreduce-1.0.0.jar -Dmapreduce.input.fileinputformat.split.maxsize=134217728 avg-tmax s3://dataset-ghcn/input/ s3://dataset-ghcn/output/avg-tmax

hadoop jar target/ghcnd-mapreduce-1.0.0.jar monthly-prcp s3://dataset-ghcn/input/ s3://dataset-ghcn/output/monthly-prcp_1

hadoop jar target/ghcnd-mapreduce-1.0.0.jar annual-max s3://dataset-ghcn/input/ s3://dataset-ghcn/output/annual-max_1
```

Hadoop exige que el directorio de salida no exista. El script
`scripts/ejecutar_emr.sh` agrega una marca temporal para evitar colisiones.

## Inspección de resultados

```bash
aws s3 cp s3://BUCKET/noaa-ghcn/output/avg-tmax/part-r-00000 - | head
aws s3 cp s3://BUCKET/noaa-ghcn/output/monthly-prcp/part-r-00000 - | head
aws s3 cp s3://BUCKET/noaa-ghcn/output/annual-max/part-r-00000 - | head
```
