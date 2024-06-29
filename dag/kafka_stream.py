import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import uuid

# default args to attach the the DAG
default_args = { 
    'owner' : 'adil',
    'start_date' : datetime.datetime(2024, 6, 29, 10, 00)
}


def get_data():
    '''Function that gets data from the api'''
    res = requests.get('https://randomuser.me/api/')
    return res.json()['results'][0]


def format_data(res):

    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    res = get_data()
    data = format_data(res)
    



# creating the dag which will serve as a entry point
with DAG(dag_id='user_automation', 
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    # Python operator
    streaming_task = PythonOperator(
        task_id = 'stram_data_from_api',
        python_callable=stream_data
    )




stream_data()
