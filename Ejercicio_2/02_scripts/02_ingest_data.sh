#!/bin/bash

# Directorio temporal donde se descargara n los archivos
TEMP_DIR="/home/hadoop/landing"

# URLs de los archivos
FILE_URLS=(
    "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"
    "https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"
)


# Nombres de los archivos correspondientes
FILE_NAMES=(
    "car_rental_data.csv"
    "georef-united-states-of-america-state.csv"
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
    wget -P "$TEMP_DIR" -O "$TEMP_DIR/$file_name" "$url"

    # Verificación de que el archivo ha sido descargado correctamente
    if [ -f "$TEMP_DIR/$file_name" ]; then
        echo "Archivo descargado correctamente: $(ls -lh $TEMP_DIR/$file_name)"
    else
        echo "Error: Fallo en la descarga del archivo."
    fi
}


# Descargar y cargar cada archivo
for i in "${!FILE_URLS[@]}"; do
    download_and_upload "${FILE_URLS[$i]}" "${FILE_NAMES[$i]}"
done