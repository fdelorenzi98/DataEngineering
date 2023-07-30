# Este es el DAG que orquesta el ETL de la tabla tipo_cambio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Crear la tabla en Amazon Redshift
TABLE_NAME = "tipo_cambio"
SCHEMA="franciscodlorenzi_coderhouse"
QUERY_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS {}.{} (
        Fecha date DISTKEY,
        Cambio double precision,
        Aumento_porcentual double precision
        process_date VARCHAR(10) DISTKEY
    )SORTKEY (Fecha);
""".format(SCHEMA, TABLE_NAME)

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM {}.{} WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
""".format(SCHEMA, TABLE_NAME)

# Crear función para obtener process_date y enviarlo a xcom
def get_process_date(**kwargs):
    # Si se proporciona la fecha del proceso se toma, sino se elige la fecha de hoy
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)

defaul_args = {
    "owner": "Francisco De Lorenzi",
    "start_date": datetime(2023, 7, 20),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "catchup": False,
    "email": ["franciscodlorenzi@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="etl_tipo_cambio",
    default_args=defaul_args,
    description="ETL de la tabla tipo_cambio",
    schedule_interval="@daily",
) as dag:
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_tipo_cambio = SparkSubmitOperator(
        task_id="spark_etl_tipo_cambio",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_tipo_cambio.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_tipo_cambio
    