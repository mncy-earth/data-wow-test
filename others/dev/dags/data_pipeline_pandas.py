from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime
from airflow.providers.postgres.hooks.postgres import PostgresHook

####################################################################################################

args = {
    'owner': 'Manutchaya W.',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

dag = DAG(
    dag_id = 'data_pipeline_pandas',
    tags = ['dev', 'dwh', 'data_sample'],
    description = 'Load parquet files from /sample/data_sample into PostgreSQL',
    default_args = args,
    dagrun_timeout = timedelta(minutes=30),
    max_active_runs = 1,
    schedule_interval = None
)

####################################################################################################

def clear_data_from_postgresql():
    ### Create PostgreSQL connection
    username = 'dwh_user'
    password = 'dwh_pass'
    database = 'postgres_db'
    postgres_engine  = create_engine(f'postgresql+psycopg2://{username}:{password}@postgres/{database}')
    with postgres_engine.connect() as connection:
        connection.execute('TRUNCATE TABLE dwh.data_sample;')
    
def extract_and_load_from_parquet():
    ### Create PostgreSQL connection
    username = 'dwh_user'
    password = 'dwh_pass'
    database = 'postgres_db'
    postgres_engine  = create_engine(f'postgresql+psycopg2://{username}:{password}@postgres/{database}')
    # postgres_connection = PostgresHook(postgres_conn_id='postgresql_airflow').get_sqlalchemy_engine()
    ### Extract data from parquet files (sample data)
    sampledata_path = Path(f"{os.getcwd()}/sampledata/data_sample")
    parquet_files = list(sampledata_path.glob('*.parquet'))
    print('no of files:', len(parquet_files))
    parquet_files = parquet_files[:5000]

    ## Define scehma (dtype for sqlalchemy)
    sampledata_schema = {'department_name':String(),
                         'sensor_serial':String(),
                         'create_at':DateTime(),
                         'product_name':String(),
                         'product_expire':DateTime()}

    for parquet_file in parquet_files:
        ### Get extracting datatime
        # extract_datetime = datetime.now()
        ### Extract sample data from parquet file
        df = pd.read_parquet(parquet_file)
        print('memory usage:', df.memory_usage(deep=True).sum(), 'bytes')
        # df['extract_at'] = extract_datetime
        ### Load data into PostgreSQL database
        df.to_sql("data_sample", con=postgres_engine, schema='dwh', if_exists='append', dtype=sampledata_schema, index=False)

    # sampledata_df = pd.concat(pd.read_parquet(parquet_file) for parquet_file in parquet_files)
    # sampledata_df['extract_at'] = extract_datetime
    ### Load data into PostgreSQL database
    # no_row_affected = sampledata_df.to_sql("data_sample", con=postgres_engine, schema='dwh', if_exists='replace', index=False)
    # print('no_row_affected:', no_row_affected)

####################################################################################################

clear_data_from_postgresql = PythonOperator(
    task_id = 'clear_data_from_postgresql',
    python_callable = clear_data_from_postgresql,
    dag = dag
)

extract_and_load_from_parquet = PythonOperator(
    task_id = 'extract_and_load_from_parquet',
    python_callable = extract_and_load_from_parquet,
    dag = dag
)

clear_data_from_postgresql >> extract_and_load_from_parquet