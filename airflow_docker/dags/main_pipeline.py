import pandas as pd
import psycopg2
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
import datetime
from datetime import timedelta
import os

# PostgreSQL Connection
POSTGRES_CONN_ID = 'postgres_localhost'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'schedule_interval': '@daily',
    'retries': 1,
}
csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'

def check_file_exist(file_path):
    return os.path.isfile(file_path)

def insert_upsert_csv_data_to_postgres():
    df = pd.read_csv(csv_filepath, sep=';')
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    conn_params = {
            'dbname': conn.schema,
            'user': conn.login,
            'password': conn.password,
            'host': conn.host,
            'port': conn.port
        }

    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    for index, row in df.iterrows():
        upsert_query = """
        INSERT INTO bronze.netflix
            (user_id, subscription_type, monthly_revenue, join_date, last_payment_date,
            country, age, gender, device, plan_duration, active_profiles,
            household_profile_ind, movies_watched, series_watched)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            subscription_type = EXCLUDED.subscription_type,
            monthly_revenue = EXCLUDED.monthly_revenue,
            join_date = EXCLUDED.join_date,
            last_payment_date = EXCLUDED.last_payment_date,
            country = EXCLUDED.country,
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            device = EXCLUDED.device,
            plan_duration = EXCLUDED.plan_duration,
            active_profiles = EXCLUDED.active_profiles,
            household_profile_ind = EXCLUDED.household_profile_ind,
            movies_watched = EXCLUDED.movies_watched,
            series_watched = EXCLUDED.series_watched
        """
        cursor.execute(upsert_query, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'import_csv_to_postgres',
    default_args=default_args,
    catchup=False ):

    upsert_task = PythonOperator(
        task_id='upsert_csv_to_postgres_task',
        python_callable=insert_upsert_csv_data_to_postgres
    )

    check_file_exist_in_path = PythonSensor(
        task_id='check_file_exist_in_path',
        python_callable=check_file_exist,
        op_args=[csv_filepath],
        mode='poke',
        timeout=600
    )   

    
    check_file_exist_in_path >> upsert_task
