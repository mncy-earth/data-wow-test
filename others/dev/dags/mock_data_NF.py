from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
from itertools import product

from sqlalchemy import create_engine

####################################################################################################

args = {
    'owner': 'Manutchaya W.',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

dag = DAG(
    dag_id = 'mock_data_pipeline',
    tags = ['test', 'dwh', 'NF'],
    description = 'Proof of normalized logic',
    default_args = args,
    dagrun_timeout = timedelta(minutes=30),
    max_active_runs = 1,
    schedule_interval = None
)

####################################################################################################

def sql_logic_test():
    ### Mock master data
    mock_master = {'department_name': ['xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2'],
                'sensor_serial': ['xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2'],
                'product_name': ['xxxxxxxxxxxxxxxx', 'aaaaaaaaaaaaaaa1', 'aaaaaaaaaaaaaaa2']
    }
    ### Create a DataFrame of all combinations of master data
    master_combinations = list(product(*mock_master.values()))
    mock_data = pd.DataFrame(master_combinations, columns=mock_master.keys())
    mock_data['create_at'] = datetime.now()
    mock_data['product_expire'] = datetime.now()
    mock_data = mock_data[['department_name', 'sensor_serial', 'create_at', 'product_name', 'product_expire']]

    ### Create PostgreSQL connection
    username = 'postgres_user'; password = 'postgres_pass'; database = 'postgres_db'
    postgres_engine  = create_engine(f'postgresql+psycopg2://{username}:{password}@postgres/{database}')

    def update_master(data, schema, table_name, column_id, column_name):
        """ Update master table (WARNING: This is a hard code for table with 2 columns)
            Return a DataFrame storing data of <schema.table_name>
        """
        ### Get ditinct values of master columns in the upcoming data
        data_names = data[column_name].unique()
        ### Get master data from PostgreSQL
        master_df = pd.read_sql(f'SELECT * FROM {schema}.{table_name}', postgres_engine)
        ### Get new master data list and latest_id (If table is empty, latest_id = 0)
        if master_df[column_name].empty:
            new_data_names = list(data_names)
            latest_id = 0
        else:
            new_data_names = list(set(data_names) - set(master_df[column_name].tolist()))
            latest_id = int(master_df[column_id].max())
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

    department_master = update_master(mock_data, 'dwh', 'master_department', 'department_id', 'department_name')
    sensor_master = update_master(mock_data, 'dwh', 'master_sensor', 'sensor_id', 'sensor_serial')
    product_master = update_master(mock_data, 'dwh', 'master_product', 'product_id', 'product_name')

    ### Transform data
    column_names = ['department_id', 'sensor_id', 'create_at', 'product_id', 'product_expire']
    df = mock_data.merge(department_master).merge(sensor_master).merge(product_master)[column_names]

    ### Load data into PostgreSQL database
    df.to_sql("data_sample", con=postgres_engine, schema='dwh', if_exists='append', index=False)

    ### Query data for checking
    query = """ SELECT d.department_name, s.sensor_serial, main.create_at, p.product_name, main.product_expire
                FROM dwh.data_sample main
                LEFT JOIN dwh.master_department d ON main.department_id = d.department_id
                LEFT JOIN dwh.master_sensor S ON main.sensor_id = s.sensor_id
                LEFT JOIN dwh.master_product p ON main.product_id = p.product_id;
            """
    with postgres_engine.connect() as connection:
        query_ouput = connection.execute(query)
    for row in query_ouput.fetchall():
        print(row)

sql_logic_test = PythonOperator(
    task_id = 'sql_logic_test',
    python_callable = sql_logic_test,
    dag = dag
)
