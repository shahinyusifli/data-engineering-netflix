from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from airflow.hooks.base_hook import BaseHook

# Define your PostgreSQL connection parameters
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

# Define the default arguments for the DAG
default_args = {
    'owner': 'shahin',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'schedule_interval': '@once',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'populate_time_dimension',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False 
)

def populate_time_dimension():
    cur = conn.cursor()
    
    query = """-- Generate a series of dates from 2021-01-01 to 2024-12-31
INSERT INTO date_dimension (ID, Day, Month, Month_Name, Year, Quarter, Weekday, Day_Of_Week_Name, Is_Weekend)
SELECT 
    d::DATE AS ID,
    EXTRACT(DAY FROM d) AS Day,
    EXTRACT(MONTH FROM d) AS Month,
    TO_CHAR(d, 'Month') AS Month_Name,
    EXTRACT(YEAR FROM d) AS Year,
    EXTRACT(QUARTER FROM d) AS Quarter,
    EXTRACT(ISODOW FROM d) AS Weekday,
    TO_CHAR(d, 'Day') AS Day_Of_Week_Name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS Is_Weekend
FROM generate_series(
    '2021-01-01'::DATE,
    '2024-12-31'::DATE,
    interval '1 day') AS d;
    """
    
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


populate_time_dimension_task = PythonOperator(
    task_id='populate_time_dimension',
    python_callable=populate_time_dimension,
    dag=dag
)


populate_time_dimension_task

