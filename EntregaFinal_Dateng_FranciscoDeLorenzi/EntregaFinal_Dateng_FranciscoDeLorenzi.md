# EntregaFinal

# Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_users.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `postgres_data/`: Carpeta con los datos de Postgres.
* `scripts/`: Carpeta con los scripts de Spark.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
    * `common.py`: Script de Spark con funciones comunes.
    * `ETL_Users.py`: Script de Spark que ejecuta el ETL.

# Pasos a seguir :

1. Desde la terminal realizar un 'git clone https://github.com/fdelorenzi98/DataEngineering.git'.
2. Crear archivo .env con el comando nano.env dentro de la carpeta EntregaFinal_Dateng_FranciscoDeLorenzi.
```bash
REDSHIFT_HOST= data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB= 
REDSHIFT_USER=
REDSHIFT_SCHEMA=
REDSHIFT_PASSWORD=
REDSHIFT_URL='jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSH>DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar'
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar

AIRFLOW_UID=501
AIRFLOW_GID=0
```
3. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p ./logs ./plugins ./postgres_data
```
4. Ejecutar el siguiente comando para levantar los servicios de Airflow y Spark en el nivel de la carpeta EntregaFinal_Dateng_FranciscoDeLorenzi. Previo a esto puede ser necesario crear las imagenes de 'airflow' y de 'spark' según se indica en cada archivo 'Dockerfile' dentro de la carpeta 'docker_images'.
```bash
docker-compose up --build
```
5. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
6. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
7. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
8. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
9. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`
10. Una vez corrido el docker-compose up --build, se creará una carpeta .config en el contenedor. Ingresar a dicha carpeta y más esepecíficamente al archivo airflow.cfg. Buscar la sección [SMTP] para configurar el protocolo de envío de mails ante fallo. 
```bash
smtp_user = "CORREO"
smtp_password = "CONTRASEÑA"

```
    El "CORREO" debe ser desde el cual deseo enviar mails, por ejemplo nuestro correo personal. Y por otro lado la "CONTRASEÑA" no es aquella que usamos para autenticarnos en el mail, sino que desde la gestión de nuestra cuenta, debemos crear una contraseña para aplicaciones, y usar el código que nos devuelven esa configuración.
11. Desde la interfaz web de Airflow, generar las variables:
```bash
SMTP_EMAIL_FROM = "CORREO"
SMTP_EMAIL_TO = "CORREO"
SMTP_PASSWORD = "CONTRASEÑA"
```
    El "CORREO" debe ser desde el cual deseo enviar mails, por ejemplo nuestro correo personal. El segundo "CORREO" debe ser hacia el cual deseo enviar mails. Y por otro lado la "CONTRASEÑA" no es aquella que usamos para autenticarnos en el mail personal, sino que desde la gestión de nuestra cuenta, debemos crear una contraseña para aplicaciones, y usar el código que nos devuelven esa configuración.
12. Se definieron umbrales para ejecutar el envío de mails cuando se supera cierto aumento porcentual diario en el tipo de cambio. Estos límites se deben configurar desde el archivo 'config.json' dentro de la carpeta 'scripts, desde el cual se indica cual es el umbral de advertencia, el intermedio y el crítico. Esto permite modificar dichos valores sin tocar el código fuente.
13. Ejecutar el DAG `etl_tipo_cambio`.
