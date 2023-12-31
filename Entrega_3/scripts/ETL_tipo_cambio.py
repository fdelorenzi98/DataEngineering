# Este script se desarrolla en Spark y realiza el proceso de ETL de la tabla tipo_cambio

import requests
from os import environ as env
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, pct_change
from pyspark.sql.types import DoubleType, DateType


from pyspark.sql.functions import concat, col, lit, when, expr, to_date

from commons import ETL_Spark

class ETL_tipo_cambio(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-20"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API y transformación
        """
        print(">>> [E] Extrayendo datos de la API...")

        # Crear una instancia de SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Obtener los datos de la API y cargarlos en un DataFrame
        url = 'https://apis.datos.gob.ar/series/api/series?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1000&sort=desc&format=json'
        response = requests.get(url, timeout=50)

        if response.status_code == 200:
            data = response.json()
            data_list = data['data']

            # Crear un DataFrame de Spark a partir de la lista de datos
            df = spark.createDataFrame(data_list)

            # Asignar nombres a las columnas
            df = df.withColumnRenamed("indice_tiempo", "fecha").withColumnRenamed("valor", "cambio")

            # Convertir la columna 'fecha' a tipo DateType
            df = df.withColumn("fecha", col("fecha").cast(DateType()))

            # Eliminar filas con fecha o cambio nulo
            df = df.dropna(subset=["fecha", "cambio"])

            # Ordenar los datos por fecha en orden ascendente
            df = df.orderBy("fecha")

            # Calcular el aumento porcentual diario del tipo de cambio
            df = df.withColumn("aumento_porcentual", pct_change("cambio").over(Window.orderBy("fecha")) * 100)

            auxi = 1

        else:
            auxi = 0
            print('Error en la solicitud:', response.status_code)

    def load(self, df, auxi):
        """
        Carga los datos transformados en Redshift
        """
        if auxi == 1 and df is not None:

            # add process_date column
            df = df.withColumn("process_date", lit(self.process_date))

            df.write \
                .format("jdbc") \
                .option("url", env['REDSHIFT_URL']) \
                .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.users") \
                .option("user", env['REDSHIFT_USER']) \
                .option("password", env['REDSHIFT_PASSWORD']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print("El proceso ha finalizado correctamente.")
    
        else:
            print('Corregir la extracción de datos para poder cargarlos a la tabla de Redshift')

if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_tipo_cambio()
    etl.run()
