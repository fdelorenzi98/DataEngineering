import pandas as pd
import requests

# Obtener los datos de la API y cargarlos en un DataFrame
url = 'https://apis.datos.gob.ar/series/api/series?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1000&sort=desc&format=json'
response = requests.get(url, timeout=1)

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

    print(df)

    auxi=1

else:
    auxi=0
    print('Error en la solicitud:', response.status_code)

if auxi == 1:
    # Carga de datos a la base de datos de Amazon Redshift usando librería SQLAlchemy
    import psycopg2
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    # Conexión
    redshift_connection = 'postgresql://franciscodlorenzi_coderhouse:994Iq7Cmn7@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database'
    # Tabla en Redshift
    table = 'tipo_cambio'
    # Esquema en Redshift
    schema = 'franciscodlorenzi_coderhouse'

    # Crear un objeto engine o conexión de SQLAlchemy
    engine = create_engine(redshift_connection)

    # Cargar los datos del DataFrame en la tabla objetivo de Redshift utilizando el método from y to de SQLAlchemy
    try:
        with engine.begin() as connection:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            df.to_sql(table, connection, schema=schema, if_exists='replace', index=False, method='multi')
        print('Carga de datos exitosa')

    except SQLAlchemyError as e:
        print('Error en la carga de datos:', str(e))

    finally:
        engine.dispose()
else:
    print('Corregir la extracción de datos para poder cargarlos a la tabla de Redshift')
