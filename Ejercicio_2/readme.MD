# Ejercicio 2 Final EDVAI

## 1 - Creacion de tablasCrear en hive una database car_rental_db y dentro una tabla llamada car_rental_analytics, con estos campos:

| Columna           | Tipo    | Descripción                      |
|-------------------|---------|----------------------------------|
| fuelType          | string  | Tipo de combustible               |
| rating            | int     | Calificación                      |
| renterTripsTaken  | int     | Viajes tomados por el arrendador  |
| reviewCount       | int     | Número de revisiones              |
| city              | string  | Ciudad                            |
| state_name        | string  | Nombre del estado                 |
| owner_id          | int     | ID del propietario                |
| rate_daily        | int     | Tarifa diaria                     |
| make              | string  | Marca del vehículo                |
| model             | string  | Modelo del vehículo               |
| year              | int     | Año del vehículo                  |


Ejecutamos lo siguiente:
``` 
hive -f /home/hadoop/scripts/Ejercicio_2/01_create_table/create_table1.sql
```

<b>SOLUCIÓN: [01_create_table/create_table1.sql](01_create_table/create_table1.sql)<b/>


## 2 - Crear script para el ingest de estos dos files
* https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D
* https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

Prueba de script para descarga de archivos
```
# Generar permisos de ejecucion
chmod +x /home/hadoop/scripts/Ejercicio_2/02_scripts/02_ingest_data.sh

# Ejecucion
./home/hadoop/scripts/Ejercicio_2/02_scripts/02_ingest_data.sh
```

<b>SOLUCIÓN: [02_scripts/02_ingest_data.sh](02_scripts/02_ingest_data.sh)<b/>

## 3 - Crear un script para tomar el archivo desde HDFS y hacer las siguientes
transformaciones:
* En donde sea necesario, modificar los nombres de las columnas. Evitar espacios y puntos (reemplazar por _ ). Evitar nombres de columna largos
* Redondear los float de ‘rating’ y castear a int.
* Joinear ambos files
* Eliminar los registros con rating nulo
* Cambiar mayúsculas por minúsculas en ‘fuelType’
* Excluir el estado Texas 
Finalmente insertar en Hive el resultado

<b>SOLUCIÓN: [03_pyspark/03_transform.py](03_pyspark/03_transform.py)<b/>

## 4 - Realizar un proceso automático en Airflow que orqueste los pipelines creados en los puntos anteriores. Crear dos tareas:

a. Un DAG padre que ingente los archivos y luego llame al DAG hijo

b. Un DAG hijo que procese la información y la cargue en Hive

<b>SOLUCIÓN: 
* DAG Padre: [04_dags/04-ingest-file-dag.py](04_dags/04-ingest-file-dag.py)
* DAG Hijo: [04_dags/05-transform-load-dag.py](04_dags/05-transform-load-dag.py)
<b/>

```
# DAG PADRE
...
trigger_transform_load_dag = TriggerDagRunOperator(
        task_id='trigger_transform_load_dag',
        trigger_dag_id='ej4-transform-load-dag',  # El ID del DAG hijo
    )

start_process >> download_files >> ingest_hdfs >> trigger_transform_load_dag
```

```
# DAG HIJO
...
with DAG(
    dag_id='ej4-transform-load-dag',
    default_args=default_args,
    description='DAG para transformar y cargar datos en Hive',
    schedule_interval=None,
    start_date=current_time + timedelta(seconds=10),
    dagrun_timeout=timedelta(minutes=60),
    tags=['transform', 'ej-4', 'edvai'],
) as dag:

...
```


## 5 - Por medio de consultas SQL al data-warehouse, mostrar:
a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.

b. los 5 estados con menor cantidad de alquileres (crear visualización)

c. los 10 modelos (junto con su marca) de autos más rentados (crear visualización)

d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015

e. las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)

f. el promedio de reviews, segmentando por tipo de combustible

<b>SOLUCIÓN: [Notebook Jupyter: 05_queries/pyspark_queries.ipynb](05_queries/pyspark_queries.ipynb), [Script: 05_queries/pyspark_queries.py](05_queries/pyspark_queries.py)<b/>

## 6 - Elabore sus conclusiones y recomendaciones sobre este proyecto.
> Conclusiones

Los datos acerca del alquiler de automóviles son cruciales para la toma de decisiones en la empresa. La capacidad de analizar estos datos puede proporcionar información valiosa sobre las preferencias de los clientes y las tendencias del mercado. Para la elaboración de reportes es necesario mejorar el trabajo sobre los datos mediante la automatización de pipeline que incluyan la ingesta, transformación y almacenamiento. Es necesario tener en cuenta el trabajo de limpieza que se debe realizar en la fase de transformación donde se realizarán imputaciones, correcciones en los nombres de los features e imputación de nulos, ya que es muy importante para evitar errores de los datos en la carga al DW,

> Recomendaciones
* Estandaricacion de nombres de features
* Documentación sobre que es cada feature y descripción de ellas

## 7 - Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud

Como Data Engineer recomendaría trabajar en la nube por:
* Escalabilidad
* Costos variables y controlados
* Mantenimeinto
* Seguridad
* Apps de ML

Una solución on-premise requeriría una inversión inicial alta en hardware, un trabajo constante de mantenimiento, y carecería de la agilidad y la capacidad de escalar rápidamente, lo que podría limitar el crecimiento y mejoras del proyecto.