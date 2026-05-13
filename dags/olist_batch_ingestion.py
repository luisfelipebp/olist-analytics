from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import glob
from pathlib import Path


from datetime import datetime, timedelta
import sys

def ingest_with_logging(file_name, file_path, **context):
    logger = logging.getLogger(__name__)
    execution_date = context['ds'] # Pega a data de execução da DAG
    hook = PostgresHook(postgres_conn_id="olist_db")
    conn = hook.get_conn()
    cursor = conn.cursor()
    row_count = 0
    table_name  = Path(file_name).stem.replace("_dataset", "")
    try:
        cursor.execute(f"TRUNCATE TABLE raw.{table_name};")

        with open(file_path, "r") as f:
            cursor.copy_expert(
                f"""
                COPY raw.{table_name}
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

        cursor.execute(f"SELECT COUNT(*) FROM raw.{table_name};")
        row_count = cursor.fetchone()[0]
        if row_count == 0:
            raise ValueError("Nenhuma linha inserida")
        conn.commit()
        return row_count
    except Exception as e:
        conn.rollback()
        logger.error(f"Falha, erro{e}")
        raise
    finally:
        logger.info(f"Run de {execution_date} inseriu {row_count} linhas na tabela {table_name}.")


default_args= {
    'owner': 'luisfelipebp',
}

with DAG(
    dag_id="ingest_with_rowcount",
    default_args=default_args,
    description="Orquestração de produtos de e-commerce",
    start_date=datetime(2026,1,1),
    schedule_interval="@daily",
    catchup=False,    
)as dag:
    
    create_raw_tables = PostgresOperator(
        task_id='create_raw_tables',
        postgres_conn_id='olist_db',
        sql='scripts/create_raw_tables.sql'
    )
    
    arquivos = glob.glob('/tmp/data/*.csv')

    ingest_task  = PythonOperator.partial(
        task_id='ingest_task',
        python_callable=ingest_with_logging
    ).expand(
        op_kwargs=[
            {"file_path": f, "file_name":os.path.basename(f)}
            for f in arquivos
        ]
    )

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test'
    )

create_raw_tables >> ingest_task >> dbt_run >> dbt_test
