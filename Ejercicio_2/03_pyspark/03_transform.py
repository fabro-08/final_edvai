from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, lower


# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Ingestar Datos de Vuelos") \
    .enableHiveSupport() \
    .getOrCreate()


car_df = spark.read.option("header", "true") \
    .csv("hdfs://172.17.0.2:9000/ingest/car_rental_data.csv")

car_df = car_df.withColumnRenamed("location.city", "city") \
    .withColumnRenamed("location.country", "country") \
    .withColumnRenamed("location.latitude", "latitude") \
    .withColumnRenamed("location.longitude", "longitude") \
    .withColumnRenamed("location.state", "state") \
    .withColumnRenamed("owner.id", "owner_id") \
    .withColumnRenamed("rate.daily", "rate_daily") \
    .withColumnRenamed("vehicle.make", "make") \
    .withColumnRenamed("vehicle.model", "model") \
    .withColumnRenamed("vehicle.type", "type") \
    .withColumnRenamed("vehicle.year", "car_year")

# Redondear y castear la columna rating a int
car_df = car_df.withColumn("rating", round(col("rating")).cast("int"))

# Eliminar registros con rating nulo
car_df = car_df.filter(col("rating").isNotNull())

# Cambiar a minúsculas las columnas relevantes
car_df = car_df.withColumn("fuelType", lower(col("fuelType"))) \
    .withColumn("city", lower(col("city"))) \
    .withColumn("state", lower(col("state"))) \
    .withColumn("make", lower(col("make"))) \
    .withColumn("model", lower(col("model"))) \
    .withColumn("type", lower(col("type")))


geo_df = spark.read.option("header", "true") \
    .option("sep",";") \
    .csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

geo_df = geo_df.withColumnRenamed("Geo Point", "geo_point") \
    .withColumnRenamed("Geo Shape", "geo_shape") \
    .withColumnRenamed("Year", "geo_year") \
    .withColumnRenamed("Official Code State", "official_code_state") \
    .withColumnRenamed("Official Name State", "official_name_state") \
    .withColumnRenamed("Iso 3166-3 Area Code", "iso_area_code") \
    .withColumnRenamed("Type", "type") \
    .withColumnRenamed("United States Postal Service state abbreviation", "state_abbr") \
    .withColumnRenamed("State FIPS Code", "state_fips_code") \
    .withColumnRenamed("State GNIS Code", "state_gnis_code")

geo_df = geo_df.withColumn("official_name_state", lower(col("official_name_state"))) \
    .withColumn("state_abbr", lower(col("state_abbr")))

# Excluir el estado de Texas
car_df = car_df.filter(col("state") != "texas")

# Asumimos que other_df tiene una columna llamada 'state' para el join
joined_df = car_df.join(geo_df, car_df.state == geo_df.state_abbr, "inner")


# Mostrar el esquema y las primeras filas del DataFrame resultante
joined_df.printSchema()
joined_df.show(5)

df_final = joined_df.select(
    col("fueltype"),
    col("rating"),
    col("rentertripstaken").cast("int"),
    col("reviewcount").cast("int"),
    col("city"),
    col("official_name_state").alias("state_name"),
    col("owner_id").cast("int"),
    col("rate_daily").cast("int"),
    col("make"),
    col("model"),
    col("car_year").cast("int").alias("year")
)

df_final.write.insertInto("car_rental_db.car_rental_analytics")

# Finalizar la sesion Spark
spark.stop()
