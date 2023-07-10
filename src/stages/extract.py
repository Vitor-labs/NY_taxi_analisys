"""
ETL Flow to extract taxi data and upload send it to the 
consumer for futher transformation
"""
from kafka import KafkaProducer
from time import time

import requests


BATCH_SIZE = 10000
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"


def download_dataset(
        url:str = DATA_URL, 
        batch_size:int = BATCH_SIZE, 
        topic:str = 'trips_data'
        ):
    """
    Downloads the trips dataset by chunks and sends then to
    the consumer aplication.

    Args:
        url (str, optional): _description_. Defaults to DATA_URL.
        batch_size (int, optional): _description_. Defaults to BATCH_SIZE.
        topic (str, optional): _description_. Defaults to 'trips_data'.
    """
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    response = requests.get(url, stream=True)
    response.raise_for_status()

    print('Enviados Chunks para o tópico: ', topic)
    start = time()
    
    for chunk in response.iter_content(chunk_size=batch_size):
        print('sending')
        producer.send(topic, chunk)

    producer.flush()

    end = time()
    print(f'Chunks enviados com sucesso para o tópico: { topic } | Tempo: {end - start}' )


download_dataset()
