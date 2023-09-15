from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from sqlalchemy import create_engine

import pandas as pd
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType

####################################################################################################

args = {
    'owner': 'Manutchaya W.',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

dag = DAG(
    dag_id = 'data_pipeline_spark',
    tags = ['dev', 'dwh', 'data_sample'],
    description = 'Load parquet files from /sample/data_sample into PostgreSQL',
    default_args = args,
    dagrun_timeout = timedelta(minutes=120),
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
    host = 'localhost'
    port = '5432'
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    ### Extract data from parquet files (sample data)
    sampledata_path = Path(f"{os.getcwd()}/sampledata/data_sample")
    # parquet_files = list(sampledata_path.glob('*.parquet'))
    sampledata_schema = (StructType()
             .add('department_name', StringType(), True)
             .add('sensor_serial', StringType(), True)
             .add('create_at', TimestampType(), True)
             .add('product_name', StringType(), True)
             .add('product_expire', TimestampType(), True)
    )
    spark = SparkSession.builder.getOrCreate()
    # sampledata_df = spark.read.schema(sampledata_schema).option("recursiveFileLookup","true").parquet(sampledata_path)
    sampledata_path += '2023_01_01_00_00_00.parquet'
    sampledata_df = spark.read.option("header","false").parquet(sampledata_path)
    ### Load data into PostgreSQL database
    sampledata_df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "dwh.data_sample").option("user", username).option("password", password).save()
    spark.stop()

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