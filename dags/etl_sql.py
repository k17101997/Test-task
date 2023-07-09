from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow import DAG
import pendulum
import psycopg2
import urllib.request
import os
import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine, DateTime

download_uris = [
"https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
"https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
"https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
"https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
]


create_new_schema_sql = """
CREATE SCHEMA test;
"""


create_dim_table_sql = """
CREATE TABLE IF NOT EXISTS test.stations(
    id int not null,
    name varchar(50),
    PRIMARY KEY (id)
);
"""


create_dim_table_tmp_sql = """
CREATE TABLE IF NOT EXISTS test.stations_tmp(
    id int not null,
    name varchar(50),
    PRIMARY KEY (id)
);
"""


create_fact_table_sql = """
CREATE TABLE IF NOT EXISTS test.trips(
    id int not null,
    start_time timestamp,
    end_time timestamp,
    bike_id int,
    trip_duration float,
    from_station_id int,
    to_station_id int,
    user_type varchar(10),
    gender varchar(6),
    birth_year int,
    PRIMARY KEY (id)
);
"""


create_fact_table_tmp_sql = """
CREATE TABLE IF NOT EXISTS test.trips_tmp(
    id int not null,
    start_time timestamp,
    end_time timestamp,
    bike_id int,
    trip_duration float,
    from_station_id int,
    to_station_id int,
    user_type varchar(10),
    gender varchar(6),
    birth_year int,
    PRIMARY KEY (id)
);
"""


merge_dim_table_sql = """
INSERT INTO test.stations
SELECT * 
FROM test.stations_tmp
ON CONFLICT (id)
    DO NOTHING;
"""


merge_fact_table_sql = """
INSERT INTO test.trips
SELECT *
FROM test.trips_tmp
ON CONFLICT (id)
    DO NOTHING;
"""


list_file_downloaded = []


def download_file(uri : str):
    file_name = uri.split('/')[-1]
    urllib.request.urlretrieve(uri, file_name)
    list_file_downloaded.append(file_name)
    return file_name


def delete_file():
    for file in list_file_downloaded:
        os.remove(file)


def execute_sql(conn, sql):
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.close()
    except Exception as e:
        return str(e)
    

def create_connection():
    conn = psycopg2.connect(database="airflow",
                        host="postgres",
                        user="airflow",
                        password="airflow",
                        port="5432"
                        )
    return conn


def process_file(conn, engine, file_name_zip, table):
    file_name_csv = file_name_zip.split('.')[0] + '.csv'
    with ZipFile(file_name_zip) as zip:
        with zip.open(file_name_csv) as myZip:
            df = pd.read_csv(myZip, index_col=False)
            df.columns = ['id', 'start_time', 'end_time', 'bike_id', 'trip_duration', 'from_station_id', 'from_station_name', 'to_station_id', 'to_station_name', 'user_type', 'gender', 'birth_year']

            if table == 'station':
                pd.concat([df[['from_station_id','from_station_name']].rename(columns={'from_station_id':'id', 'from_station_name': 'name'}),
                                            df[['to_station_id', 'to_station_name']].rename(columns={'to_station_id':'id', 'to_station_name': 'name'})], ) \
                    .drop_duplicates().to_sql(con=engine, name='stations_tmp', chunksize=10000, if_exists='replace', method='multi', schema='test', index=False)
                execute_sql(conn, merge_dim_table_sql)

            if table == 'trip':
                df['trip_duration'] = df['trip_duration'].str.replace(',', '').astype(float)
                df.drop(['from_station_name', 'to_station_name'], axis=1)\
                    .to_sql(con=engine, name='trips_tmp', chunksize=10000, if_exists='replace', method='multi', schema='test', index=False, dtype={'start_time': DateTime(), 'end_time': DateTime()})
                execute_sql(conn, merge_fact_table_sql)
            


conn = create_connection()
conn.autocommit = True
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

with DAG(
    dag_id="etl_sql",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['test', 'etl'],
) as dag:
    start_task = EmptyOperator(
        task_id="Start"
    )
    
    @task(task_id = 'create_schema')
    def create_schema():
        execute_sql(conn, create_new_schema_sql)

    @task(task_id = 'create_station_table')
    def create_station_table():
        execute_sql(conn, create_dim_table_sql)

    @task(task_id = 'create_station_tmp_table')
    def create_station_tmp_table():
        execute_sql(conn, create_dim_table_tmp_sql)

    @task(task_id = 'create_trip_table')
    def create_trip_table():
        execute_sql(conn, create_fact_table_sql)

    @task(task_id = 'create_trip_tmp_table')
    def create_trip_tmp_table():
        execute_sql(conn, create_fact_table_tmp_sql)

    @task(task_id = 'import_station_data')
    def import_station_data():
        for uri in download_uris:
            file_name_zip = download_file(uri)
            process_file(conn, engine, file_name_zip, 'station')

    @task(task_id = 'import_trip_data')
    def import_trip_data():
        for uri in download_uris:
            file_name_zip = download_file(uri)
            process_file(conn, engine, file_name_zip, 'trip')

    @task(task_id = 'delete_file_task')
    def delete_file_task():
        delete_file()

    tmp_task = EmptyOperator(
        task_id="Tmp"
    )

    end_task = EmptyOperator(
        task_id="End"
    )

    start_task >> create_schema() >> [create_station_table(), create_station_tmp_table(), create_trip_table(), create_trip_tmp_table()] >> tmp_task
    tmp_task >> [import_station_data(), import_trip_data()] >> delete_file_task() >> end_task