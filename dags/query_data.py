from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import pandas as pd

####################################################################################################

args = {
    'owner': 'Manutchaya W.',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

dag = DAG(
    dag_id = 'query_data_pipeline',
    tags = ['test', 'dwh', 'data_sample'],
    description = 'Query data from PostgreSQL',
    default_args = args,
    dagrun_timeout = timedelta(minutes=30),
    max_active_runs = 1,
    schedule_interval = None
)

####################################################################################################

def query_create_at_count(postgres_url, pool_timeout_sec):
    ### Prepare PostgreSQL connection
    postgres_engine = create_engine(postgres_url, pool_timeout=pool_timeout_sec)
    ## Query data
    query = """ SELECT create_at::VARCHAR(10), count(DISTINCT create_at), count(*)
                FROM dwh.data_sample GROUP BY 1;
            """
    # query = 'SELECT * FROM pg_shadow;'
    with postgres_engine.connect() as connection:
        query_ouput = connection.execute(query)
    for row in query_ouput.fetchall():
        print(row)

def query_data_count(postgres_url, pool_timeout_sec):
    ### Prepare PostgreSQL connection
    postgres_engine = create_engine(postgres_url, pool_timeout=pool_timeout_sec)
    ### Query data for checking
    query = """ SELECT count(*) AS total_rows
                ,      count(DISTINCT department_name) AS no_departments
                ,      count(DISTINCT sensor_serial) AS no_sensors
                ,      count(DISTINCT product_name) AS no_products
                FROM dwh.data_sample;
            """
    query_output = pd.read_sql(query, postgres_engine)
    print(query_output.to_dict(orient='records'))

####################################################################################################

username = 'dwh_user'; password = 'dwh_pass'; database = 'postgres_db'
postgres_url = f'postgresql+psycopg2://{username}:{password}@postgres/{database}'
pool_timeout_sec = 5*60

start = EmptyOperator(task_id="start", dag=dag)

query_create_at_count = PythonOperator(
    task_id = 'query_create_at_count',
    python_callable = query_create_at_count,
    op_kwargs = {'postgres_url': postgres_url, 'pool_timeout_sec': pool_timeout_sec},
    dag = dag
)

query_data_count = PythonOperator(
    task_id = 'query_data_count',
    python_callable = query_data_count,
    op_kwargs = {'postgres_url': postgres_url, 'pool_timeout_sec': pool_timeout_sec},
    dag = dag
)

end = EmptyOperator(task_id="end", dag=dag)

start >> [query_create_at_count, query_data_count] >> end