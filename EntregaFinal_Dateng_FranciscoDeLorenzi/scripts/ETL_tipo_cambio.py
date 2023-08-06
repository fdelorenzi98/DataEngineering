# Este script se desarrolla en Spark y realiza el proceso de ETL de la tabla tipo_cambio

from os import environ as env
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, pct_change
from pyspark.sql.types import DoubleType, DateType
from pyspark.sql.functions import concat, col, lit, when, expr, to_date
from commons import ETL_Spark
from datetime import datetime
from pyspark.sql.window import Window
from airflow.models import Variable
import smtplib
import requests
import json

class ETL_tipo_cambio(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = self.process_date
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

            # Obtener el aumento porcentual del día de hoy
            today_aumento_porcentual = df.filter(df["fecha"] == expr("current_date()")).select("aumento_porcentual").collect()

            # Cargar el archivo de configuración con los thresholds
            with open('config.json', 'r') as config_file:
                config = json.load(config_file)
            thresholds = config["thresholds"]

            # Verificar si el aumento porcentual del día de hoy supera el primer threshold
            if today_aumento_porcentual and today_aumento_porcentual[0]["aumento_porcentual"] > float(list(thresholds.keys())[0]):
                aumento_porcentual_hoy = today_aumento_porcentual[0]["aumento_porcentual"]
            else:
                aumento_porcentual_hoy = None

            return df, aumento_porcentual_hoy
        
        else:
            print('Error en la solicitud:', response.status_code, '. No se procedió a la carga de datos en Redshift.')

    def send(self, aumento_porcentual_hoy):
        try:
            # Carga los umbrales y mensajes desde el archivo de configuración
            with open('config.json', 'r') as config_file:
                config = json.load(config_file)
            thresholds = config["thresholds"]

            if aumento_porcentual_hoy is not None:
                x = smtplib.SMTP('smtp.gmail.com', 587)
                x.starttls()
                x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))

                # Recorre el diccionario de umbrales para verificar si el aumento porcentual supera alguno de ellos
                for threshold, message_template in thresholds.items():
                    if aumento_porcentual_hoy > float(threshold):
                        subject = 'Movimiento atípico del tipo de cambio'
                        body_text = aumento_porcentual_hoy
                        message = message_template.format(body_text)
                        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'), message)
                        print('Exito')
                        break  # Detiene el bucle si ya se envió un correo

                x.quit()

        except Exception as e:
            print(e)
            print('Error')
            raise Exception

    def load(self, df):
        """
        Carga los datos transformados en Redshift
        """
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
            
        print("El proceso de carga de datos ha finalizado correctamente.")

if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_tipo_cambio()
    etl.run()
    