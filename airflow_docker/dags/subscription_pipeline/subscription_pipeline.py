from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from airflow.hooks.base_hook import BaseHook

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

default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'schedule_interval': '@daily',
    'retries': 1
}

def extract_data():
    csv_filepath = '/opt/airflow/dags/Netflix_dataset.csv'
    df = pd.read_csv(csv_filepath, sep=";")
   
    return df

def load_subscriptions_to_dim():
    df = extract_data()

    extracted_subscription = df['Subscription Type'].unique()
    extracted_revenue = df["Monthly Revenue"].unique()
    extracted_plan_duration = df["Plan Duration"].unique()
    modified_plan_duration = [duration.replace(" Month", "") for duration in extracted_plan_duration]
    cur = conn.cursor()

    for subscription in extracted_subscription:
        for revenue in extracted_revenue:
            for plan_duration in modified_plan_duration:
                # Convert plan_duration and revenue to Python integers
                plan_duration_int = int(plan_duration)
                revenue_int = int(revenue)
                
                cur.execute("SELECT * FROM SubscriptionDimension WHERE subscription = %s and plan_duration = %s and revenue = %s", (subscription, plan_duration_int, revenue_int))
                existing_data = cur.fetchone()
                
                if existing_data is None:
                    cur.execute("INSERT INTO SubscriptionDimension (subscription, plan_duration, revenue) VALUES (%s, %s, %s)", (subscription, plan_duration_int, revenue_int))
    
    conn.commit()
    conn.close()



dag = DAG(
    'subscription_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval according to your needs
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_load_subscriptions_to_dim = PythonOperator(
    task_id='load_subscriptions_to_dim',
    python_callable=load_subscriptions_to_dim,
    dag=dag,
)


# Define task dependencies
task_extract_data >> task_load_subscriptions_to_dim
