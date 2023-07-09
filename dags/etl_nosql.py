import urllib.request, json
import pymongo
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow import DAG


myclient = pymongo.MongoClient("mongodb://mongo:27017/")
db = myclient['test']


def insert_data(type):
    collection = db[type]
    data = get_data(type)
    collection.insert_many(data)


def get_data(type):
    url = "https://api.covidtracking.com/v2/us/daily.json"
    with urllib.request.urlopen(url) as url:
        data = json.load(url)
        if type == 'meta':
            for meta in data['meta']['field_definitions']:
                meta['build_time'] = data['meta']['build_time']
                meta['license'] = data['meta']['license']
                meta['version'] = data['meta']['version']
            return data['meta']['field_definitions']
        elif type == 'data':
            return data['data']
        else :
            return []



with DAG(
    dag_id="etl_nosql",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["nosql", 'test'],
) as dag:
    start_task = EmptyOperator(
        task_id="Start"
    )

    @task(task_id = 'import_meta_task')
    def import_meta_task():
        insert_data('meta')

    @task(task_id = 'import_data_task')
    def import_data_task():
        insert_data('data')

    end_task = EmptyOperator(
        task_id="End"
    )

    start_task >> import_meta_task() >> end_task
    start_task >> import_data_task() >> end_task