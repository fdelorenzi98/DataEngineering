import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import SparkSession
from psycopg2 import sql

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
    
else:
    auxi = 0
    print('Error en la solicitud:', response.status_code)

if auxi == 1:
    # Crear una instancia de SparkSession
    spark = SparkSession.builder.getOrCreate()

    spark_df = spark.createDataFrame(df)
    spark_df.show(n=10)
    
    # Convertir DataFrame de Spark a lista
    tipos_cambio = spark_df.collect()
    
    # Datos de conexión a Amazon Redshift
    host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    port = 5439
    database = "data-engineer-database"
    user = "franciscodlorenzi_coderhouse"
    password = "994Iq7Cmn7"
    schema = "franciscodlorenzi_coderhouse"

    # Crear la conexión a Amazon Redshift
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password
    )

    # Crear el cursor
    cursor = conn.cursor()
    
    # Crear la tabla en Amazon Redshift
    table_name = "tipo_cambio"
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {}.{} (
            Fecha date DISTKEY,
            Cambio double precision,
            Aumento_porcentual double precision
        )SORTKEY (Fecha);
    """.format(schema, table_name)

    cursor.execute(create_table_query)
    conn.commit()
    
    # Insertar los datos en la tabla
    for tipo_de_cambio in tipos_cambio:
        fecha = tipo_de_cambio.fecha

        # Verificar si los datos ya existen en la tabla
        select_query = sql.SQL("""
            SELECT COUNT(*) FROM {}.{}
            WHERE Fecha = %s
        """).format(sql.Identifier(schema), sql.Identifier(table_name))

        cursor.execute(select_query, (fecha,))
        result = cursor.fetchone()

        if result[0] == 0:
            # Los datos no existen, realizar la inserción
            insert_query = sql.SQL("""
                INSERT INTO {}.{} (Fecha, Cambio, Aumento_porcentual)
                VALUES (%s, %s, %s)
            """).format(sql.Identifier(schema), sql.Identifier(table_name))

            cursor.execute(insert_query, (fecha, cambio, aumento_porcentual))
            conn.commit()
        else:
            print("Los datos con fecha {} ya existen en la tabla.".format(fecha))

    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()

    print("El proceso ha finalizado correctamente.")
    
else:
    print('Corregir la extracción de datos para poder cargarlos a la tabla de Redshift')
