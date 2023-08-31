from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'schedule_interval': '15 0 * * *',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 

with DAG(
    'bronze_to_silver',
    default_args=default_args,
    catchup=False 
):

    upsert_query = """
        merge into silver.netflix snf
        using bronze.netflix bnf
        on snf.user_id = bnf.user_id
        when matched then
        update set 
        subscription_type = bnf.subscription_type,
        monthly_revenue = bnf.monthly_revenue,
        join_date = TO_CHAR(TO_DATE(bnf.join_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')::DATE,
        last_payment_date = TO_CHAR(TO_DATE(bnf.last_payment_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')::DATE,
        country = bnf.country,
        age = bnf.age,
        gender = bnf.gender,
        device = bnf.device,
        plan_duration = CAST(SPLIT_PART(bnf.plan_duration, ' ', 1) AS INTEGER),
        active_profiles = bnf.active_profiles,
        household_profile_ind = bnf.household_profile_ind,
        movies_watched = bnf.movies_watched,
        series_watched = bnf.series_watched
        when not matched then
        insert (user_id, subscription_type, monthly_revenue, join_date, last_payment_date, country, age, gender, device, plan_duration, active_profiles, household_profile_ind, movies_watched, series_watched)
        values (bnf.user_id, bnf.subscription_type, bnf.monthly_revenue, TO_CHAR(TO_DATE(bnf.join_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')::DATE, TO_CHAR(TO_DATE(bnf.last_payment_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')::DATE, bnf.country, bnf.age, bnf.gender, bnf.device, CAST(SPLIT_PART(bnf.plan_duration, ' ', 1) AS INTEGER), bnf.active_profiles, bnf.household_profile_ind, bnf.movies_watched, bnf.series_watched);
            """

    delete_outlier_query = """ 
        DELETE FROM silver.netflix
        WHERE age > 110; """


    bronze_to_silver = PostgresOperator(
        task_id='bronze_to_silver',
        sql=upsert_query,
        postgres_conn_id=conn_id 
    )

    clean_outlier_with_age = PostgresOperator(
        task_id='clean_outlier_with_age',
        sql=delete_outlier_query,
        postgres_conn_id=conn_id 
    )


    wait_for_main_etl = ExternalTaskSensor(
        task_id='triger_bronze_to_silver',
        external_dag_id='import_csv_to_postgres',
        external_task_id='upsert_csv_to_postgres_task',
        execution_delta = timedelta(minutes=5),
        timeout=600)
    
    bronze_to_silver >> clean_outlier_with_age 
