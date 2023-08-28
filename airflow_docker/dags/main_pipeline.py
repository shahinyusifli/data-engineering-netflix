import pandas as pd
import psycopg2
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# PostgreSQL Connection
POSTGRES_CONN_ID = 'postgres_localhost'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,  # Set your preferred schedule
    catchup=False,
)

# Python function to load CSV and insert/upsert data into PostgreSQL
def insert_upsert_csv_data_to_postgres():
    # Load CSV data using Pandas
    csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
    df = pd.read_csv(csv_filepath, sep=';')

    # Get PostgreSQL connection parameters from Airflow connection
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    conn_params = {
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'host': conn.host,
        'port': conn.port
    }

    # Insert or upsert data into PostgreSQL using INSERT ON CONFLICT
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

# Create an upsert task to execute the Python function
upsert_task = PythonOperator(
    task_id='upsert_csv_to_postgres_task',
    python_callable=insert_upsert_csv_data_to_postgres,
    dag=dag,
)

success_sensor_task = SqlSensor(
    task_id='success_main_etl_task',
    conn_id=POSTGRES_CONN_ID,
    sql="SELECT 1",
    mode="poke",
    timeout=600,  # Adjust as needed
    poke_interval=30,  # Adjust as needed
    retries=5,  # Adjust as needed
    dag=dag,
)

# Set task dependencies
upsert_task >> success_sensor_task
