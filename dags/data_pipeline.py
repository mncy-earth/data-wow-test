from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from sqlalchemy import create_engine

import pandas as pd
import os
from pathlib import Path
# from numba import jit, cuda

####################################################################################################

args = {
    'owner': 'Manutchaya W.',
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    dag_id = 'data_pipeline',
    tags = ['prod', 'dwh', 'data_sample'],
    description = 'Load parquet files from /sample/data_sample into PostgreSQL',
    default_args = args,
    dagrun_timeout = timedelta(minutes=2*60),
    max_active_runs = 1,
    schedule_interval = None
)

####################################################################################################

def clear_data_from_postgresql():
    ### Create PostgreSQL connection
    username = 'dwh_user'; password = 'dwh_pass'; database = 'postgres_db'
    postgres_engine  = create_engine(f'postgresql+psycopg2://{username}:{password}@postgres/{database}')
    with postgres_engine.connect() as connection:
        connection.execute('TRUNCATE TABLE dwh.data_sample;')

# @jit(target_backend='cuda')
def extract_and_load_from_parquet():
    ### Prepare PostgreSQL connection
    username = 'dwh_user'; password = 'dwh_pass'; database = 'postgres_db'
    postgres_url = f'postgresql+psycopg2://{username}:{password}@postgres/{database}'
    postgres_engine = create_engine(postgres_url, pool_timeout=1800)
    ### List file path
    sampledata_path = Path(f"{os.getcwd()}/sampledata/data_sample")
    parquet_files = list(sampledata_path.glob('*.parquet'))
    ### Merge sample data every 6 hours (1 batch = 360 parquet files = 689,040 rows)
    ### Merge sample data every 8 hours (1 batch = 480 parquet files = 918,720 rows)
    ### Merge sample data every 12 hours (1 batch = 720 parquet files = 1,378,080 rows)
    ### Merge sample data each day (1 batch = 1,440 parquet files = 2,756,160 rows)
    no_files = 12*60
    batches = [parquet_files[idx:idx+no_files] for idx in range(0, len(parquet_files), no_files)]
    print('no of batches:', len(batches))
    ### ETL
    for batch in batches:
        print(batch[0], 'to', batch[-1], 'are loading')
        ### Extract sample data from parquet file
        df = pd.concat(pd.read_parquet(parquet_file) for parquet_file in batch)
        ### Load data into PostgreSQL database
        df.to_sql("data_sample", con=postgres_engine, schema='dwh', if_exists='append', index=False, method='multi')

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
