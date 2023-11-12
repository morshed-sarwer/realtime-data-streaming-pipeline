from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
from kafka import KafkaProducer


def load_user_data():
    import requests

    api_url = 'https://randomuser.me/api/'
    res = requests.get(api_url)
    res = res.json()
    return res['results'][0]
    # print(json.dumps(res['results'],indent=3))
  
def format_data(res):
    data = {}
    data['first_name']=res['name']['first']
    data['last_name']=res['name']['last']
    location = res['location']
    # data['id'] = res['login']['uuid'],
    data['gender']=res['gender']
    data['post_code']=res['location']['postcode']
    data['email']=res['email']
    data['user_name']=res['login']['username']
    data['dob']=res['dob']['date']
    data['registered_date']=res['registered']['date']
    data['phone']=res['phone']
    data['picture']=res['picture']['medium']
    data['address']=f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    
    return data
      
def stream_data():
    import json
    import logging
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=50000)
    
    
    # for i in range(2000):
    #     try:
    #         data = load_user_data()
    #         data = format_data(data)
    #         producer.send('user_data_producer',json.dumps(data).encode('utf-8'))
    #     except Exception as e:
    #         logging.error(f'error occured {e}')
    #         continue
    while True:
        try:
            data = load_user_data()
            data = format_data(data)
            producer.send('user_data_producer',json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f'error occured {e}')
            continue
    

default_args = {
    'owner':'morshed',
    'start_date':datetime(2023,1,1,00)
}

dag=DAG(
    dag_id='kafka_user_data_producer',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

user_task = PythonOperator(
    task_id='random_user_task',
    python_callable=stream_data,
    dag=dag
)

user_task