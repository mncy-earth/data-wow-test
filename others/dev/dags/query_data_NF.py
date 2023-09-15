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
    dag_id = 'query_data_pipeline_NF',
    tags = ['test', 'dwh', 'NF'],
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

def query_joined_data_example(postgres_url, pool_timeout_sec):
    ### Prepare PostgreSQL connection
    postgres_engine = create_engine(postgres_url, pool_timeout=pool_timeout_sec)
    ### Query data for checking
    query = """ SELECT d.department_name, s.sensor_serial, main.create_at, p.product_name, main.product_expire
                FROM dwh.data_sample main
                LEFT JOIN dwh.master_department d ON main.department_id = d.department_id
                LEFT JOIN dwh.master_sensor S ON main.sensor_id = s.sensor_id
                LEFT JOIN dwh.master_product p ON main.product_id = p.product_id
                LIMIT 5
            """
    query_output = pd.read_sql(query, postgres_engine)
    print(query_output.to_dict(orient='records'))

def query_joined_data_count(postgres_url, pool_timeout_sec):
    ### Prepare PostgreSQL connection
    postgres_engine = create_engine(postgres_url, pool_timeout=pool_timeout_sec)
    ### Query data for checking
    query = """ SELECT count(*) AS total_rows
                ,      count(DISTINCT department_name) AS no_departments
                ,      count(DISTINCT sensor_serial) AS no_sensors
                ,      count(DISTINCT product_name) AS no_products
                FROM prod.data_sample;
            """
    query_output = pd.read_sql(query, postgres_engine)
    print(query_output.to_dict(orient='records'))

def query_master_data_count(postgres_url, pool_timeout_sec):
    ### Prepare PostgreSQL connection
    postgres_engine = create_engine(postgres_url, pool_timeout=pool_timeout_sec)
    ### Query data for checking
    department_query = "SELECT COUNT(1) FROM dwh.master_department;"
    sensor_query = "SELECT COUNT(1) FROM dwh.master_sensor;"
    product_query = "SELECT COUNT(1) FROM dwh.master_product;"
    with postgres_engine.connect() as connection:
        department_query_ouput = connection.execute(department_query)
        sensor_query_ouput = connection.execute(sensor_query)
        product_query_ouput = connection.execute(product_query)
    print('no_departments:', department_query_ouput.fetchall())
    print('no_sensors:', sensor_query_ouput.fetchall())
    print('no_products:', product_query_ouput.fetchall())

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

query_joined_data_example = PythonOperator(
    task_id = 'query_joined_data_example',
    python_callable = query_joined_data_example,
    op_kwargs = {'postgres_url': postgres_url, 'pool_timeout_sec': pool_timeout_sec},
    dag = dag
)

query_joined_data_count = PythonOperator(
    task_id = 'query_joined_data_count',
    python_callable = query_joined_data_count,
    op_kwargs = {'postgres_url': postgres_url, 'pool_timeout_sec': pool_timeout_sec},
    dag = dag
)

query_master_data_count = PythonOperator(
    task_id = 'query_master_data_count',
    python_callable = query_master_data_count,
    op_kwargs = {'postgres_url': postgres_url, 'pool_timeout_sec': pool_timeout_sec},
    dag = dag
)

end = EmptyOperator(task_id="end", dag=dag)

start >> [query_create_at_count, query_joined_data_example, query_joined_data_count, query_master_data_count] >> end