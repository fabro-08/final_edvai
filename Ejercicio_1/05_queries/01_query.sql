-- Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022
SELECT COUNT(*)
FROM aeropuerto_detalles_tabla_transform
WHERE fecha BETWEEN '2021-12-01' AND '2022-01-31';
