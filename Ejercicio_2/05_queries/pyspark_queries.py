from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, year, desc, to_date

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Car Rental Analytics") \
    .enableHiveSupport() \
    .getOrCreate()

# Leer la tabla desde Hive
df = spark.sql("SELECT * FROM car_rental_analytics")

# a. Cantidad de alquileres de autos ecológicos (hibrido o eléctrico) con un rating de al menos 4
eco_rentals_count = df.filter((col("fueltype").isin("hybrid", "electric")) & (col("rating") >= 4)).count()
print(f"Cantidad de alquileres de autos ecológicos con un rating de al menos 4: {eco_rentals_count}") #3288


# b. Los 5 estados con menor cantidad de alquileres
state_rentals = df.groupBy("state_name").count().orderBy(col("count").asc()).limit(5)
state_rentals.show()


# c. Los 10 modelos (junto con su marca) de autos más rentados
model_rentals = df.groupBy("make", "model").count().orderBy(col("count").desc()).limit(10)
model_rentals.show()

# d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015
# Convertir el año a un tipo de fecha temporal
df_with_date = df.withColumn("year_date", to_date(col("year").cast("string"), "yyyy"))

# Filtrar y agrupar por año
yearly_rentals = df_with_date.filter((col("year") >= 2010) & (col("year") <= 2015)) \
    .groupBy(year("year_date").alias("year")).count().orderBy("year")

yearly_rentals.show()


# e. Las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)
eco_city_rentals = df.filter(col("fueltype").isin("hybrid", "electric")) \
    .groupBy("city").count().orderBy(col("count").desc()).limit(5)
eco_city_rentals.show()


# f. El promedio de reviews, segmentando por tipo de combustible
average_reviews = df.groupBy("fueltype").agg(avg("reviewcount").alias("average_reviews"))
average_reviews.show()

# Finalizar la SparkSession
spark.stop()