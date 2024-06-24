from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, upper

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("ConsultaAeropuertos") \
    .enableHiveSupport() \
    .getOrCreate()

# Cargar la tabla
df = spark.sql("SELECT * FROM aviaciondb.aeropuerto_detalles_tabla_transform")


# Ejercicio 6
# Filtrar y contar los vuelos
df_filtered = df.filter((col('fecha') >= '2021-12-01') & (col('fecha') <= '2022-01-31'))
cantidad_vuelos = df_filtered.count()

print("Cantidad de vuelos entre 01/12/2021 y 31/01/2022:", cantidad_vuelos) #rta 3040848

# Ejercicio 7
# Filtrar los datos y sumar los pasajeros
df_filtered = df.filter((col('fecha') >= '2021-01-01') & (col('fecha') <= '2022-06-30') & (upper(col('aerolinea_nombre')).contains('AEROLINEAS ARGENTINAS')))
cantidad_pasajeros = df_filtered.groupBy().sum('pasajeros').collect()[0][0]

print("Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre 01/01/2021 y 30/06/2022:", cantidad_pasajeros)#rta  50620496

# Ejercicio 8
# Filtrar y seleccionar columnas específicas
# Filtrar los vuelos entre las fechas especificadas y que sean despegues
df_filtered_salida = df.filter(
    (col('fecha') >= '2022-01-01') & 
    (col('fecha') <= '2022-06-30') & 
    (col('tipo_de_movimiento') == 'Despegue')
)

# Seleccionar las columnas necesarias
df_selected_salida = df_filtered_salida.select(
    col('fecha'),
    col('horautc').alias('hora'),
    col('aeropuerto').alias('codigo_aeropuerto_salida'),
    col('origen_destino').alias('ciudad_salida'),
    col('pasajeros')
)

# Filtrar los vuelos que son arribos entre las fechas especificadas y que sean aterrizajes
df_filtered_llegada = df.filter(
    (col('fecha') >= '2022-01-01') & 
    (col('fecha') <= '2022-06-30') & 
    (col('tipo_de_movimiento') == 'Aterrizaje')
)

# Seleccionar las columnas necesarias
df_selected_llegada = df_filtered_llegada.select(
    col('fecha'),
    col('horautc').alias('hora'),
    col('aeropuerto').alias('codigo_aeropuerto_arrivo'),
    col('origen_destino').alias('ciudad_arrivo'),
    col('pasajeros')
)

# Mostrar el resultado ordenado por fecha de manera descendente
df_union = df_selected_salida.unionByName(df_selected_llegada, allowMissingColumns=True)
df_result = df_union.orderBy(col('fecha').desc())
df_result.show(5)


# Ejercicio 9
# Filtrar los datos, agrupar por aerolínea y sumar los pasajeros
df_filtered = df.filter((col('fecha') >= '2021-01-01') & (col('fecha') <= '2022-06-30') & (col('aerolinea_nombre').isNotNull()))
df_result = df_filtered.groupBy('aerolinea_nombre').sum('pasajeros').orderBy(desc('sum(pasajeros)')).limit(10)

df_result.show(5)


# Ejercicio 10
# Filtrar los datos, agrupar por aeronave y contar los vuelos
df_filtered = df.filter((col('fecha') >= '2021-01-01') & (col('fecha') <= '2022-06-30') & (upper(col('origen_destino')).contains('AEP') | upper(col('origen_destino')).contains('EZE') ) & (col('aeronave').isNotNull()))
df_result = df_filtered.groupBy('aeronave').count().orderBy(desc('count')).limit(10)

df_result.show(5)


# Finalizar la SparkSession
spark.stop()