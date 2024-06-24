-- Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022

SELECT SUM(pasajeros) AS total_pasajeros
FROM aeropuerto_detalles_tabla_transform
WHERE aerolinea_nombre = 'Aerolíneas Argentinas'
  AND fecha BETWEEN '2021-01-01' AND '2022-06-30';
