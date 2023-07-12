"""
Module Docstring
"""
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer


def consume_dataset(topic:str = 'trips_data'):
    """
    Consumes the queue data to transform

    Args:
        topic (str, optional): Defaults to 'trips_data'.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='my_consumer_group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print('Listening...')

    for message in consumer:
        chunk = message.value
        json_df = pd.read_json(chunk, orient='records')
        
        formated_df = process_dataset(json_df)
        formated_df = pd.concat([formated_df, json_df], ignore_index=True)

        load_dataset(formated_df) # sends data to GCP Big Query

    consumer.close()

consume_dataset() # remove

def process_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process each column from dataset to make the fact_table
    and other dimensional tables
    """
    datetime_dim = (
    df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']]
    .drop_duplicates()
    .reset_index(drop=True)
    .assign(
        pick_hour=lambda x: x['tpep_pickup_datetime'].dt.hour,
        pick_day=lambda x: x['tpep_pickup_datetime'].dt.day,
        pick_month=lambda x: x['tpep_pickup_datetime'].dt.month,
        pick_year=lambda x: x['tpep_pickup_datetime'].dt.year,
        pick_weekday=lambda x: x['tpep_pickup_datetime'].dt.weekday,
        drop_hour=lambda x: x['tpep_dropoff_datetime'].dt.hour,
        drop_day=lambda x: x['tpep_dropoff_datetime'].dt.day,
        drop_month=lambda x: x['tpep_dropoff_datetime'].dt.month,
        drop_year=lambda x: x['tpep_dropoff_datetime'].dt.year,
        drop_weekday=lambda x: x['tpep_dropoff_datetime'].dt.weekday,
        datetime_id=lambda x: x.index
    )
    .reindex(columns=[
        'datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month',
        'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day',
        'drop_month', 'drop_year', 'drop_weekday'
        ])
    )

    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }

    rate_code_dim = (
        df[['RatecodeID']]
        .drop_duplicates()
        .assign(rate_code_id=lambda x: x.index)
        .assign(rate_code_name=lambda x: x['RatecodeID'].map(rate_code_type))
        .rename(columns={'RatecodeID': 'rate_code_id'})
        [['rate_code_id', 'rate_code_id', 'rate_code_name']]
    )

    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }

    payment_type_dim = (
        df[['payment_type']]
        .drop_duplicates()
        .replace({'payment_type': payment_type_name})
        .rename(columns={'payment_type': 'payment_type_name'})
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={'index': 'payment_type_id'})
        [['payment_type_id', 'payment_type_name']]
    )

    df_locations = pd.read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')

    pickup_location_dim = pd.DataFrame()
    pickup_location_dim['LocationID'] = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pd.merge(pickup_location_dim, df_locations, on='LocationID')
    pickup_location_dim = pickup_location_dim.rename(columns={'LocationID':'PULocationID'})

    dropoff_location_dim = pd.DataFrame()
    dropoff_location_dim['LocationID'] = df[['DOLocationID']] \
        .drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = pd.merge(dropoff_location_dim, df_locations, on='LocationID')
    dropoff_location_dim = dropoff_location_dim.rename(columns={'LocationID':'DOLocationID'})

    fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                .merge(trip_distance_dim, on='trip_distance') \
                .merge(rate_code_dim, on='RatecodeID') \
                .merge(pickup_location_dim, on='PULocationID') \
                .merge(dropoff_location_dim, on='DOLocationID')\
                .merge(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
                .merge(payment_type_dim, on='payment_type') \
                [['VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 
                'dropoff_location_id', 'payment_type_id', 'ffare_amount', 'extra', 'mta_tax', 
                'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
                'congestion_surcharge', 'airport_fee']]

    return fact_table

def load_dataset(fact_table: pd.DataFrame) -> None:
    """
    Send data already procesed to the load step, to be
    sended to big query
    """
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for _, row in fact_table.iterrows():
        row_dict = row.to_dict()

        producer.send('fact_table', value=str(row_dict).encode('utf-8'))

    producer.flush()
    producer.close()
