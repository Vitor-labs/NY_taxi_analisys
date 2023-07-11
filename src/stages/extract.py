"""
ETL Flow to extract taxi data and upload send it to the 
consumer for futher transformation
"""
from time import time
from kafka import KafkaProducer

import pyarrow.parquet as pq
import pandas as pd

import requests
import tempfile
import os


BATCH_SIZE = 2048
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

    trips = pd.read_parquet(url)
    total_rows = len(trips)
    num_batches = total_rows // batch_size + 1

    start = time()

    print('Sending Chunks to: ', topic)

    for i in range(num_batches):
        t_start = time()
        start = i * batch_size
        end = min((i + 1) * batch_size, total_rows)

        chunk = trips.iloc[start:end]
        json_data = chunk.to_json(orient='records')

        print('sending to...', end)
        producer.send(topic, json_data.encode('utf-8'))

        print(f'...Inserted {end} lines')
        break

    t_end = time()
    print(f'Done. Process time: {t_end - t_start} seconds')

    producer.flush()
    producer.close()

download_dataset()
