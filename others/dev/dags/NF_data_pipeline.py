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
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

dag = DAG(
    dag_id = 'data_pipeline_NF',
    tags = ['dev', 'dwh', 'NF'],
    description = 'Load parquet files from /sample/data_sample into PostgreSQL',
    default_args = args,
    dagrun_timeout = timedelta(minutes=120),
    max_active_runs = 1,
    schedule_interval = None
)

####################################################################################################

def clear_data_from_postgresql():
    ############# !!!!!!!!!!!!!!!! dont forget clearing master tables
    ### Create PostgreSQL connection
    username = 'dwh_user'; password = 'dwh_pass'; database = 'postgres_db'
    postgres_engine  = create_engine(f'postgresql+psycopg2://{username}:{password}@postgres/{database}')
    with postgres_engine.connect() as connection:
        connection.execute('TRUNCATE TABLE dwh.data_sample;')

# @jit(target_backend='cuda')
def etl_task():

    def update_master(data, master_df, latest_id, schema, table_name, column_id, column_name):
        """ Update master table if there is new data (WARNING: This is a hard code for table with 2 columns)
            Return a DataFrame storing data of <schema.table_name>
        """
        ### Get ditinct values of master columns in the upcoming data
        data_names = data[column_name].unique()
        ### Get new master data list
        new_data_names = data_names if master_df.empty else list(set(data_names) - set(master_df[column_name].tolist()))
        ### If any new master data
        if new_data_names:
            ### Get new ids for new master data
            new_data_ids = list(range(latest_id+1, len(new_data_names)+latest_id+1))
            ### Get DataFrame of new master data
            new_master_df = pd.DataFrame({column_id:new_data_ids, column_name: new_data_names})
            ### Update master_df
            master_df = pd.concat([master_df, new_master_df], ignore_index=True)
            ### Insert new master data into master tables
            new_master_df['values'] = new_master_df.apply(lambda row: f"({row[column_id]}, '{row[column_name]}')", axis=1)
            sql_query = f"INSERT INTO {schema}.{table_name} ({column_id}, {column_name}) VALUES {', '.join(new_master_df['values'])};"
            with postgres_engine.connect() as connection:
                connection.execute(sql_query)
        return master_df

    ### Prepare PostgreSQL connection
    username = 'dwh_user'; password = 'dwh_pass'; database = 'postgres_db'
    postgres_url = f'postgresql+psycopg2://{username}:{password}@postgres/{database}'
    postgres_engine = create_engine(postgres_url, pool_timeout=60)
    ### List file path
    sampledata_path = Path(f"{os.getcwd()}/sampledata/data_sample")
    parquet_files = list(sampledata_path.glob('*.parquet'))
    ### Merge sample data every 6 hours (1 batch = 360 parquet files = 689,040 rows)
    ### Merge sample data every 8 hours (1 batch = 480 parquet files = 918,720 rows)
    ### Merge sample data every 12 hours (1 batch = 720 parquet files = 1,378,080 rows)
    ### Merge sample data each day (1 batch = 1,440 parquet files = 2,756,160 rows)
    no_files = 6*60
    batches = [parquet_files[idx:idx+no_files] for idx in range(0, len(parquet_files), no_files)]
    print('no of batches:', len(batches))
    ### ETL
    ### Get master data from PostgreSQL
    department_master = pd.read_sql(f'SELECT * FROM dwh.master_department', postgres_engine)
    sensor_master = pd.read_sql(f'SELECT * FROM dwh.master_sensor', postgres_engine)
    product_master = pd.read_sql(f'SELECT * FROM dwh.master_product', postgres_engine)
    ### Get latest_id (If table is empty, latest_id = 0)
    department_id = 0 if department_master['department_id'].empty else int(department_master['department_id'].max())
    sensor_id = 0 if sensor_master['sensor_id'].empty else int(sensor_master['sensor_id'].max())
    product_id = 0 if product_master['product_id'].empty else int(product_master['product_id'].max())
    for idx, batch in enumerate(batches):
        print(idx, ':::', batch[0], 'to', batch[-1], 'are loading')
        ### Extract data from parquet file
        data_df = pd.concat(pd.read_parquet(parquet_file) for parquet_file in batch)
        ### Transform data
        department_master = update_master(data_df, department_master, department_id, 'dwh', 'master_department', 'department_id', 'department_name')
        sensor_master = update_master(data_df, sensor_master, sensor_id, 'dwh', 'master_sensor', 'sensor_id', 'sensor_serial')
        product_master = update_master(data_df, product_master, product_id, 'dwh', 'master_product', 'product_id', 'product_name')
        column_names = ['department_id', 'sensor_id', 'create_at', 'product_id', 'product_expire']
        df = data_df.merge(department_master).merge(sensor_master).merge(product_master)[column_names]
        ### Load data into PostgreSQL database
        df.to_sql("data_sample", con=postgres_engine, schema='dwh', if_exists='append', index=False)

####################################################################################################

clear_data_from_postgresql = PythonOperator(
    task_id = 'clear_data_from_postgresql',
    python_callable = clear_data_from_postgresql,
    dag = dag
)

etl_task = PythonOperator(
    task_id = 'etl_task',
    python_callable = etl_task,
    dag = dag
)

clear_data_from_postgresql >> etl_task
