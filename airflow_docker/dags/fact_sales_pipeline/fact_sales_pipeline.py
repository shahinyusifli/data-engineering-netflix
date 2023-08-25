from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
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
    df = df[df['Age'] <= 110]
    return df

def convert_subscription(row):
    if row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 10:
        return 1
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 15:
        return 2
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 12:
        return 3
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 13:
        return 4
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 11:
        return 5
    elif row['Subscription Type'] == 'Basic' and row['Monthly Revenue'] == 14:
        return 6
    
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 10:
        return 7
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 15:
        return 8
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 12:
        return 9
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 13:
        return 10
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 11:
        return 11
    elif row['Subscription Type'] == 'Premium' and row['Monthly Revenue'] == 14:
        return 12
    
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 10:
        return 13
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 15:
        return 14
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 12:
        return 15
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 13:
        return 16
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 11:
        return 17
    elif row['Subscription Type'] == 'Standard' and row['Monthly Revenue'] == 14:
        return 18

def mapping_subscription():
    df = extract_data()
    df['subscriptionid'] = df.apply(convert_subscription, axis=1)
    return df['subscriptionid'].tolist()

def format_last_payment_date():
    df = extract_data()
    df['Last Payment Date'] = pd.to_datetime(df['Last Payment Date'])
    formated_date = df['Last Payment Date'].dt.strftime('%Y-%m-%d').tolist()
    return formated_date

def mapping_country():
    df = extract_data()

    cur = conn.cursor()

    query = "select id, country from country_dimension"
    cur.execute(query)
    countries_dict = {row[1]: row[0] for row in cur.fetchall()}
    formated_country = df['Country'].map(countries_dict).tolist()

    return formated_country

def mapping_device():
    df = extract_data()

    cur = conn.cursor()

    query = "select id, device from device_dimension"
    cur.execute(query)
    devices_dict = {row[1]: row[0] for row in cur.fetchall()}
    formated_device = df['Device'].map(devices_dict).tolist()

    return formated_device


def load_data_to_fact_table():
    df = extract_data()
    df["subscriptionid"] = mapping_subscription()
    df["last_payment_date"] = format_last_payment_date()
    df['country_id'] = mapping_country()
    df['device_id'] = mapping_device()
    df.columns = df.columns.str.strip()
    df["subscriptionid"] = df["subscriptionid"].astype(int)

    cur = conn.cursor()

    for index, row in df.iterrows():
        subscription_exists = check_subscription_exists(row['subscriptionid']) 
        country_exists = check_country_exists(row['country_id']) 
        device_exists = check_device_exists(row['device_id'])

        if subscription_exists and country_exists and device_exists:
            cur.execute("""INSERT INTO Sales_Fact (User_ID, Subscription_ID, Last_Payment_Date, Country_ID, Device_ID) 
                        VALUES (%s, %s, %s, %s, %s)""",
                        (row['User ID'], row['subscriptionid'], row['last_payment_date'], row['country_id'], row['device_id'],))

    conn.commit()
    conn.close()


def check_subscription_exists(subscription_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM Subscription_Dimension WHERE ID = %s"
    cur.execute(query, (subscription_id,))
    count = cur.fetchone()[0]
    return count > 0

def check_country_exists(country_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM Country_Dimension WHERE ID = %s"
    cur.execute(query, (country_id,))
    count = cur.fetchone()[0]
    return count > 0

def check_device_exists(device_id):
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM Device_Dimension WHERE ID = %s"
    cur.execute(query, (device_id,))
    count = cur.fetchone()[0]
    return count > 0


dag = DAG(
    'fact_sales_pipeline',
    default_args=default_args,
    schedule_interval=None, 
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_mapping_subscription = PythonOperator(
    task_id='mapping_subscription',
    python_callable=mapping_subscription,
    dag=dag,
)

task_format_last_payment_date = PythonOperator(
    task_id='format_last_payment_date',
    python_callable=format_last_payment_date,
    dag=dag,
)

task_mapping_country = PythonOperator(
    task_id='mapping_country',
    python_callable=mapping_country,
    dag=dag,
)

task_mapping_device = PythonOperator(
    task_id='mapping_device',
    python_callable=mapping_device,
    dag=dag,
)

task_load_data_to_fact_table = PythonOperator(
    task_id='load_data_to_fact_table',
    python_callable=load_data_to_fact_table,
    dag=dag,
)



task_extract_data >> [task_mapping_subscription, task_format_last_payment_date, task_mapping_country, task_mapping_device] >> task_load_data_to_fact_table 

