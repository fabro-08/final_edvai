-- Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo,
-- y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendente

SELECT 
    d.fecha, 
    d.horautc AS hora,
    d.origen_destino AS codigo_aeropuerto_salida,
    a1.denominacion AS ciudad_salida,
    d.aeropuerto AS codigo_aeropuerto_arribo,
    a2.denominacion AS ciudad_arribo,
    d.pasajeros
FROM 
    aeropuerto_detalles_tabla_transform d
JOIN 
    aeropuerto_tabla_transform a1 ON d.origen_destino = a1.iata
JOIN 
    aeropuerto_tabla_transform a2 ON d.aeropuerto = a2.iata
WHERE 
    d.fecha BETWEEN '2022-01-01' AND '2022-06-30'
ORDER BY 
    d.fecha DESC;
