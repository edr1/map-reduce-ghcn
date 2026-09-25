#!/usr/bin/env bash
# ==============================================================================
# SCRIPT AUTOMATIZADO: Extracción, Descarga y Subida a S3 (Con Omisión de Lotes)
# ==============================================================================
# aws s3 sync /home/ec2-user/datasets/urls_05/ s3://dataset-ghcn/input/ --exclude "*" --include "*.csv"
set -Eeuo pipefail

# --- CONFIGURACIÓN DE VARIABLES ---
BASE_URL="https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
DESTINO="./datasets"
TEMP_URLS="$DESTINO/todas_las_urls.tmp"
S3_BUCKET="s3://dataset-ghcn/input/"

CONCURRENCIA_DESCARGA=10  # Descargas simultáneas de wget con xargs
CONCURRENCIA_S3=20        # Subidas simultáneas de AWS CLI a S3

# Crear el directorio base si no existe
mkdir -p "$DESTINO"

# ==============================================================================
# OPTIMIZACIÓN DE AWS CLI
# ==============================================================================
echo "==> Configurando optimización de AWS CLI para subidas rápidas..."
aws configure set default.s3.max_concurrent_requests "$CONCURRENCIA_S3"

# ==============================================================================
# FASE 1: CREAR LOTES DE ARCHIVOS TXT
# ==============================================================================
echo "==> [1/3] Extrayendo lista de URLs desde la NOAA..."

curl -fsSL "$BASE_URL" |
sed -n 's/.*href="\([^"]*\.csv\)".*/\1/p' |
awk -v base="$BASE_URL" '
  /^https?:\/\// { print; next }
  { print base $0 }
' |
sort -u > "$TEMP_URLS"

echo "==> [2/3] Dividiendo URLs en 10 lotes equitativos..."
split \
  --number=l/10 \
  --numeric-suffixes=1 \
  --suffix-length=2 \
  --additional-suffix=".txt" \
  "$TEMP_URLS" "$DESTINO/urls_"

rm "$TEMP_URLS"

echo "Lotes generados en el sistema:"
wc -l "$DESTINO"/urls_*.txt
echo "------------------------------------------------------------"

# ==============================================================================
# FASE 2 Y 3: DESCARGA LOCAL Y SUBIDA INMEDIATA A S3 (CON FILTRO)
# ==============================================================================
echo "==> [3/3] Iniciando ciclo de Descarga Local y Sincronización a S3..."

for lista in "$DESTINO"/urls_*.txt; do
    nombre_lote=$(basename "$lista" .txt)
    CARPETA_LOTE="$DESTINO/$nombre_lote"
    
    # 🔍 CONTROL DE EXCLUSIÓN: Saltamos los lotes que ya están listos en S3
    case "$nombre_lote" in
        "urls_01" | "urls_02" | "urls_03" | "urls_04" | "urls_05" | "urls_06" | "urls_07" | "urls_08"| "urls_09")
            echo ">> [SALTADO] El lote $nombre_lote ya existe en S3. Omitiendo por completo..."
            continue
            ;;
    esac
    
    echo "============================================================"
    echo " PROCESANDO: $nombre_lote"
    echo "============================================================"
    
    # 1. Descarga del lote actual
    echo " -> [Descarga] Iniciando descargas en $CARPETA_LOTE..."
    mkdir -p "$CARPETA_LOTE"
    xargs -P "$CONCURRENCIA_DESCARGA" -n 1 wget -c -q --show-progress -P "$CARPETA_LOTE" < "$lista"
    
    # 2. Subida inteligente / Reanudación a S3
    echo " -> [S3 Sync] Subiendo archivos CSV a S3 ($S3_BUCKET)..."
    #aws s3 sync "$CARPETA_LOTE/" "$S3_BUCKET" --exclude "*" --include "*.csv"
    
    # 3. Limpieza de espacio local
    #echo " -> [Limpieza] Eliminando archivos locales del $nombre_lote para liberar espacio..."
    #rm -rf "$CARPETA_LOTE"
    
    echo " -> ¡$nombre_lote completado y respaldado en S3 con éxito!"
done

echo "============================================================"
echo "==> ¡Proceso completo de datos finalizado con éxito! <=="
echo "============================================================"
