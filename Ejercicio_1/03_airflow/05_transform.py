  
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Transformar Datos en Hive") \
    .enableHiveSupport() \
    .getOrCreate()


# Cargar los datos desde Hive
aeropuerto_detalles_df = spark.sql("SELECT * FROM aviaciondb.aeropuerto_detalles_tabla")
aeropuerto_df = spark.sql("SELECT * FROM aviaciondb.aeropuerto_tabla")

# Filtra solamente vuelos domesticos
ad_df_filtered = aeropuerto_detalles_df.filter(aeropuerto_detalles_df.clasificacion_de_vuelo == "Doméstico")

# Convertir valores NULL a 0 en campos espec      ficos
ad_df_filtered_cleaned = ad_df_filtered.withColumn('pasajeros', when(ad_df_filtered.pasajeros.isNull(), 0).otherwise(ad_df_filtered.pasajeros))


a_df_filtered_cleaned = aeropuerto_df.withColumn('distancia_ref', when(aeropuerto_df.distancia_ref.isNull(), 0).otherwise(aeropuerto_df.distancia_ref))


# Guardar los datos transformados de nuevo en Hive
ad_df_filtered_cleaned.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("aviaciondb.aeropuerto_detalles_tabla_transform")

a_df_filtered_cleaned.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("aviaciondb.aeropuerto_tabla_transform")

# Detener la SparkSession
spark.stop()

#scp /path/to/local/aeropuertos_detalle.csv hadoop@192.168.1.100:/home/hadoop/remote_directory/
#sshpass -p 'edvai' scp create_table1.sql hadoop@172.17.0.2:/home/hadoop/scripts/create_table2_1.sql
