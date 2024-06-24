-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS car_rental_db;

-- Usar la base de datos creada
USE car_rental_db;

CREATE EXTERNAL TABLE car_rental_analytics (
    fuelType STRING,
    rating INT,
    renterTripsTaken INT,
    reviewCount INT,
    city STRING,
    state_name STRING,
    owner_id INT,
    rate_daily INT,
    make STRING,
    model STRING,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tables/external/car_rental_db';