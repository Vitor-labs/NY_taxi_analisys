from kafka import KafkaConsumer
from sqlalchemy import create_engine
import hvac

client = hvac.Client(url='http://localhost:8200')

read_user_response = client.secrets.kv.v2.read_secret_version(path='db')
read_password_response = client.secrets.kv.v2.read_secret_version(path='db')
read_host_response = client.secrets.kv.v2.read_secret_version(path='db')
read_port_response = client.secrets.kv.v2.read_secret_version(path='db')
read_database_response = client.secrets.kv.v2.read_secret_version(path='db')
read_table_response = client.secrets.kv.v2.read_secret_version(path='db')

bootstrap_servers = 'localhost:9092'
topic = 'trips_data'

def load_on_database():
    """
    Loads transformated data on postgres database
    """
    user = read_user_response['data']['data']['user']
    password = read_password_response['data']['data']['password']
    host = read_host_response['data']['data']['host']
    port = read_port_response['data']['data']['port']
    database = read_database_response['data']['data']['database']
    table = read_table_response['data']['data']['table']

    consumer = KafkaConsumer(topic,
                            bootstrap_servers=bootstrap_servers,
                            )

    DB_URL = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(DB_URL)
    engine.connect()

    for message in consumer:
        message_value = message.value.decode('utf-8')
        columns = message_value.split(',')
        
        # Finish the loading

    consumer.close()
