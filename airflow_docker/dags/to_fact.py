from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'schedule_interval': '@daily',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 


with DAG(
    'to_fact_sales',
    default_args=default_args,
    catchup=False
):

    to_fct_sales_query = """
    merge into gold.fct_sales gfs
    using silver.netflix snf
    on gfs.account_id = snf.user_id
    when matched then
    update set 
    subscription_id = gold.map_subscription_to_id(snf.subscription_type, snf.monthly_revenue),
    last_payment_date = snf.last_payment_date,
    device_id = gold.map_device_to_id(snf.device),
    active_profiles = snf.active_profiles,
    household_profile_ind = snf.household_profile_ind,
    movies_watched = snf.movies_watched,
    series_watched = snf.series_watched

    when not matched then
    insert (account_id, subscription_id, last_payment_date, device_id, active_profiles, household_profile_ind, movies_watched, series_watched)
    values (snf.user_id, gold.map_subscription_to_id(snf.subscription_type, snf.monthly_revenue), snf.last_payment_date, gold.map_device_to_id(snf.device), snf.active_profiles, snf.household_profile_ind, snf.movies_watched, snf.series_watched);"""


    to_fct_sales = PostgresOperator(
        task_id='to_fct_sales',
        sql=to_fct_sales_query,
        postgres_conn_id=conn_id)

    to_fct_sales
