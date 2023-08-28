from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'schedule_interval': '@daily',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 


dag = DAG(
    'to_dim_account',
    default_args=default_args,
    schedule_interval=None, 
)



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
values (snf.user_id, snf.join_date, snf.age, snf.gender, snf.country);
"""


to_dim_account = PostgresOperator(
    task_id='to_dim_account',
    sql=to_dim_account_query,
    postgres_conn_id=conn_id, 
    dag=dag 
)

to_dim_account
