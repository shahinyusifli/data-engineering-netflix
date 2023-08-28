from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
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


dag = DAG(
    'to_dim_subscription',
    default_args=default_args,
    schedule_interval=None, 
)
to_dim_subscription_query = """
WITH extracted_values AS (
    SELECT DISTINCT subscription_type,
                    monthly_revenue,
                    plan_duration
    FROM silver.netflix
)

INSERT INTO gold.dim_subscription (subscription_type, revenue, plan_duration)
SELECT ev.subscription_type, ev.monthly_revenue, ev.plan_duration
FROM extracted_values ev
LEFT JOIN  gold.dim_subscription sd
    ON ev.subscription_type = sd.subscription_type
    AND ev.monthly_revenue = sd.revenue
    AND ev.plan_duration = sd.plan_duration
WHERE sd.subscription_type IS NULL;
"""



to_dim_subscription = PostgresOperator(
    task_id='to_dim_subscription',
    sql=to_dim_subscription_query,
    postgres_conn_id=conn_id, 
    dag=dag 
)

to_dim_subscription
