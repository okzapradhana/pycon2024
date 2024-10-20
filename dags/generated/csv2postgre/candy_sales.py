from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from datetime import timedelta, datetime
from airflow.exceptions import AirflowFailException
import polars as pl
import json
import os
import textwrap
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DOC_MD = """
This DAG is generated through generator script that processes a Jinja template.
Please don't modify this python code directly, consider to modify the Jinja template if you want to make any changes.
"""

default_args = {
    'owner': 'okza.pradhana',
    'start_date': datetime.strptime('2024-10-18', '%Y-%m-%d'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=5),
}

def _extract_data(source_filepath: str):
    df = pl.read_csv(source_filepath)

    return {
        'rowcount': df.select(pl.len()).item(0,0),
        'data': df.write_json()
    }

def _load(
        data: json,
        table: str,
        if_table_exist: bool,        
    ):
    df = pl.read_json(data)

    logger.info(f"Writing data to {table}")
    df.write_database(
        table_name=table,
        # NOTE: consider to spin up another postgres instance, this is just for the sake of demo
        connection=os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'), 
        if_table_exists=if_table_exist
    )
    logger.info("Writing data success")

def _check(
        **kwargs        
    ):
    task_instance = kwargs['ti']
    source_count = task_instance.xcom_pull('extract', 'rowcount')
    dest_count = task_instance.xcom_pull('get_table_count')

    if source_count == dest_count:
        logger.info('The number of rows of source and destination are match!')
        return True
    else:
        return AirflowFailException('The number of rows of source and destination are not same!')

with DAG(
    dag_id='csv2postgre_candy_sales',
    description='Sequence of jobs to ingest CSV files to candy_sales in Postgres. This DAG is created through generator script.',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['etl', 'template', 'csv2postgre'],
    dagrun_timeout=timedelta(minutes=10),
    doc_md=DOC_MD
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    extract_csv = PythonOperator(
        task_id="extract",
        python_callable=_extract_data,
        op_args=[os.path.join(os.environ.get('AIRFLOW_HOME'), 'datasource', 'candy_sales.csv')]
    )

    load_to_pg = PythonOperator(
        task_id="load",
        python_callable=_load,
        op_kwargs={
            'data': '{{ ti.xcom_pull("extract_csv", "data")) }}',
            'table': 'candy_sales',
            'if_table_exist': True
        }
    )

    get_table_count = SQLExecuteQueryOperator(
        task_id="count",
        sql=textwrap.dedent("""
            SELECT
                COUNT(*)
            FROM candy_sales
        """),
        conn_id="dwh_postgres"
    )

    rowcount_check = PythonOperator(
        task_id="rowcount_check",
        python_callable=_check
    )

    start >> extract_csv >> load_to_pg >> get_table_count >> rowcount_check >> end