#!/bin/bash

# Directorio temporal donde se descargara n los archivos
TEMP_DIR="/home/hadoop/landing"

# URLs de los archivos
FILE_URLS=(
    "https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv"
    "https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv"
    "https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv"
)

# Nombres de los archivos correspondientes
FILE_NAMES=(
    "2021-informe-ministerio.csv"
    "202206-informe-ministerio.csv"
    "aeropuertos_detalle.csv"
)

# Creacion del directorio temporal si no existe
mkdir -p $TEMP_DIR

# Cambio al directorio temporal
cd $TEMP_DIR

# Funcion para descargar y cargar archivos a HDFS
download_and_upload() {
    local url=$1
    local file_name=$2

    # Descarga del archivo usando wget
    wget "$url" -O "$file_name"

    # Verificacion de que el archivo ha sido descargado correctamente
    if [ -f "$file_name" ]; then
        echo "Archivo descargado correctamente: $(ls -lh $file_name)"
    else
        echo "Error: Fallo en la descarga del archivo."
    fi
}
