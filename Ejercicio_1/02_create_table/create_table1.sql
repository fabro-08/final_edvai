-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS aviaciondb;

-- Usar la base de datos creada
USE aviaciondb;

CREATE EXTERNAL TABLE aeropuerto_detalles_tabla (
    fecha DATE,
    horaUTC STRING,
    clase_de_vuelo STRING,
    clasificacion_de_vuelo STRING,
    tipo_de_movimiento STRING,
    aeropuerto STRING,
    origen_destino STRING,
    aerolinea_nombre STRING,
    aeronave STRING,
    pasajeros INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tables/external/aviaciondb';
