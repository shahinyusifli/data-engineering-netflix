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
    'schedule_interval': '20 0 * * *',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 


with DAG(
    'to_dim_account',
    default_args=default_args,
    catchup=False 
):

    to_dim_account_query = """
    merge into gold.dim_account gdc
    using silver.netflix snf
    on gdc.id = snf.user_id
    when matched then
    update set 
    join_date = snf.join_date,
    age = snf.age,
    gender = snf.gender,
    country = snf.country
    when not matched then
    insert (id, join_date, age, gender, country)
    values (snf.user_id, snf.join_date, snf.age, snf.gender, snf.country);"""


    to_dim_account = PostgresOperator(  
        task_id='to_dim_account',
        sql=to_dim_account_query,
        postgres_conn_id=conn_id,  
    )

    column_checks = SQLColumnCheckOperator(
        task_id="column_checks",
        conn_id=conn_id,
        table="gold.dim_account",
        column_mapping={
            "join_date": {
                "null_check": {"equal_to": 0} 
            },
            "age": {
                "null_check": {"equal_to": 0},
                "max": {"geq_to": 110}
            },
            "gender": {
                "null_check": {"equal_to": 0}
            },
            "country": {
                "null_check": {"equal_to": 0}
            }
        }
    )

    to_dim_account >> column_checks
