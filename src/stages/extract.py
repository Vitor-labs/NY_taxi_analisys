"""
ETL Flow to extract taxi data and upload send it to the 
consumer for futher transformation
"""
from time import time
from kafka import KafkaProducer

import pandas as pd
import hvac


client = hvac.Client(url='http://localhost:8200')

BATCH_SIZE = 10000 # not modify
DATA_URL = client.secrets.kv.v2.read_secret_version(path='url')
TOPIC = 'trips_data'


def download_dataset(
        url:str = DATA_URL,
        batch_size:int = BATCH_SIZE,
        topic:str = TOPIC
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

    trips = pd.read_parquet(url)
    total_rows = len(trips)
    num_batches = total_rows // batch_size + 1

    if producer.bootstrap_connected():
        print('Sending Chunks to:', topic)

    start = time()

    for i in range(num_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, total_rows)

        chunk = trips.iloc[start:end]
        json_data = chunk.to_json(orient='records')

        producer.send(topic=topic, value=json_data.encode('utf-8'))

    end = time()
    print(f'Done. Process time: {end - start} seconds')

    producer.flush()
    producer.close()

download_dataset() #remove
