from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from . import settings


###############################################
# DAG Definition
###############################################

DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(settings.NOW.year, settings.NOW.month, settings.NOW.day, settings.DELAYS_PROCESSING_HOUR),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
SPARK_ENVIRONMENT = {
    'MONGO_URL': settings.MONGO_URL,
    'MSSQL_HOST': settings.MSSQL_HOST,
    'SQLSERVER_USERNAME': settings.SQLSERVER_USERNAME,
    'SQLSERVER_PASSWORD': settings.SQLSERVER_PASSWORD,
}

with DAG(
        dag_id="delays_processing",
        description="This DAG runs a Pyspark app that uses modules.",
        default_args=DAG_DEFAULT_ARGS,
        schedule_interval=timedelta(1)
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/opt/spark/execution_scripts/delays_proc.py",  # Spark app path created in airflow & spark cluster
        application_args=[settings.TRAM_FOLDER],
        name="delays processing",
        conn_id="spark_default",
        conf={"spark.master": settings.SPARK_MASTER},
        packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre8",
        dag=dag,
        env_vars=SPARK_ENVIRONMENT
    )

    task_move_to_archive = BashOperator(
        task_id="move_to_archive",
        bash_command=f'mkdir -p {settings.SPARK_OUT_DIR}ARCHIVE/ '
                     f'&& mv {settings.TRAM_FOLDER}* {settings.SPARK_OUT_DIR}ARCHIVE/ '
                     f'&& rm -R {settings.TRAM_FOLDER}'
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> spark_job >> task_move_to_archive >> end
