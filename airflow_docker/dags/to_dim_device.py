from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'schedule_interval': '25 0 * * *',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 


with DAG(
    'to_dim_device',
    default_args=default_args,
    catchup=False
):

    to_dim_device_query = """
            INSERT INTO gold.dim_device (device)
            SELECT DISTINCT device
            FROM silver.netflix
            WHERE device NOT IN (SELECT device FROM gold.dim_device);
            """

    to_dim_device = PostgresOperator(
        task_id='to_dim_device',
        sql=to_dim_device_query,
        postgres_conn_id=conn_id 
    )

    column_checks = SQLColumnCheckOperator(
        task_id="column_checks",
        conn_id=conn_id,
        table="gold.dim_device",
        column_mapping={
            "device": {
                "null_check": {"equal_to": 0},
                "distinct_check": {"equal_to": 4} 
            }
        }
    )

    to_dim_device >> column_checks
