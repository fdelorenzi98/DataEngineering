# Este script se desarrolla en Spark y realiza el proceso de ETL de la tabla tipo_cambio

import pandas as pd
import requests
import psycopg2
from datetime import datetime, timedelta
from os import environ as env
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import SparkSession
from psycopg2 import sql


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

        # Obtener los datos de la API y cargarlos en un DataFrame
        url = 'https://apis.datos.gob.ar/series/api/series?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1000&sort=desc&format=json'
        response = requests.get(url, timeout=50)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data['data'])

            # Asignar nombres a las columnas
            df.columns = ['fecha', 'cambio']

            # Evaluar y eliminar duplicados basados en la columna 'fecha'
            df = df.drop_duplicates(subset=['fecha'])

            # Eliminar filas con fecha o cambio nulo
            df = df.dropna(subset=['fecha', 'cambio'])

            # Convertir la columna 'fecha' a tipo datetime
            df['fecha'] = pd.to_datetime(df['fecha'])

            # Ordenar los datos por fecha en orden ascendente
            df = df.sort_values('fecha')

            # Calcular el aumento porcentual diario del tipo de cambio
            df['aumento_porcentual'] = df['cambio'].pct_change() * 100

            auxi = 1

            return df

        else:
            auxi = 0
            print('Error en la solicitud:', response.status_code)

    def load(self, df, auxi):
        """
        Carga los datos transformados en Redshift
        """
        if auxi == 1 and df is not None:

            # Crear una instancia de SparkSession
            spark = SparkSession.builder.getOrCreate()

            # Convertir el DataFrame de Pandas a DataFrame de Spark
            spark_df = spark.createDataFrame(df)

            # add process_date column
            spark_df = spark_df.withColumn("process_date", lit(self.process_date))

            spark_df.write \
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
