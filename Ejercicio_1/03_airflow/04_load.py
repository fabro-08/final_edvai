from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.types import DateType

# Crear sesion Spark
spark = SparkSession.builder \
    .appName("Ingestar Datos de Vuelos") \
    .enableHiveSupport() \
    .getOrCreate()

# Definir esquema para los archivos de vuelos
schema_vuelos = "fecha STRING, horautc STRING, clase_de_vuelo STRING, clasificacion_de_vuelo STRING, tipo_de_movimiento STRING, aeropuerto STRING, origen_destino STRING, aerolinea_nombre STRING, aeronave STRING, pasajeros INT"

print("antes de leer primer csv")
# Leer datos de los archivos CSV en DataFrames
vuelos_2021_df = spark.read.option("header", "true") \
    .option("sep",";") \
    .schema(schema_vuelos) \
    .csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
print("Leyo con exitooo")

vuelos_2021_df_formatdate = vuelos_2021_df.withColumn('fecha',to_date(col("fecha"),"dd/mm/yyyy"))

vuelos_2022_df = spark.read.option("header", "true") \
    .option("sep",";") \
    .schema(schema_vuelos) \
    .csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")

vuelos_2021_df_formatdate = vuelos_2022_df.withColumn('fecha',to_date(col("fecha"),"dd/mm/yyyy"))

# Filtrar datos entre las fechas 01/01/2021 y 30/06/2022
vuelos_union = vuelos_2021_df_formatdate.union(vuelos_2021_df_formatdate)
vuelos_filtrados_df = vuelos_union.filter((vuelos_union.fecha >= "2021-01-01") & (vuelos_union.fecha <= "2022-06-30"))

vuelos_filtrados_df.show(5)
# Guardar datos filtrados en la tabla de Hive
vuelos_filtrados_df.write.insertInto("aviaciondb.aeropuerto_detalles_tabla")

# Leer datos del archivo de detalles de aeropuertos en un DataFrame
schema_aeropuertos = "aeropuerto STRING, oac STRING, iata STRING, tipo STRING, denominacion STRING, coordenadas STRING, latitud STRING, longitud STRING, elev FLOAT, uom_elev STRING, ref STRING, distancia_ref FLOAT, direccion_ref STRING, condicion STRING, control STRING, region STRING, uso STRING, trafico STRING, sna STRING, concesionado STRING, provincia STRING"

aeropuertos_detalles_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep",";") \
    .schema(schema_aeropuertos) \
    .csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

aeropuertos_detalles_df.show(5)
# Guardar datos en la tabla de Hive
aeropuertos_detalles_df.write.insertInto("aviaciondb.aeropuerto_tabla")

# Finalizar la sesion Spark
spark.stop()