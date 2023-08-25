from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import json
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'schedule_interval': '@monthly',
    'retries': 1
}

conn_id = 'postgres_localhost'
conn = BaseHook.get_connection(conn_id) 

conn_params = {
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'host': conn.host,
        'port': conn.port
    }
    
conn = psycopg2.connect(**conn_params)

def extract_data():
    csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
    df = pd.read_csv(csv_filepath, sep=";")
   
    return df

def detect_anomalies():
    df = extract_data()
    extracted_countries = df['Country'].unique()
    with open('/opt/airflow/dags/country_pipeline/countries.json', 'r') as file:
        countries_data = json.load(file)
    countries_list = countries_data['countries']

    for s in extracted_countries:
        if s not in countries_list:
            print(f'{s} is invalid for country name')

def load_countries_to_dim():
    df = extract_data()
    
    unique_countries = df['Country'].unique()
    cur = conn.cursor()

    for country in unique_countries:
        cur.execute("SELECT * FROM Country_Dimension WHERE country = %s", (country,))
        existing_data = cur.fetchone()

        if existing_data is None:
            cur.execute("INSERT INTO Country_Dimension (Country) VALUES (%s)", (country,))
    
    conn.commit()
    conn.close()


dag = DAG(
    'country_pipeline',
    default_args=default_args,
    schedule_interval=None, 
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_countries_to_dim = PythonOperator(
    task_id='load_countries_to_dim',
    python_callable=load_countries_to_dim,
    dag=dag,
)

task_detect_anomalies = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    dag=dag,
)


task_extract_data >> task_detect_anomalies >> task_load_countries_to_dim
