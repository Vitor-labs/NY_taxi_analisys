import pandas as pd

from kafka import KafkaConsumer


def format_tables(value):
    df = pd.DataFrame(value)
    ...

def consume_dataset(topic:str = 'trips_data'):
    """
    Consumes the queue data to transform

    Args:
        topic (str, optional): Defaults to 'trips_data'.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='my_consumer_group'
    )

    for message in consumer:
        chunk = message.value
        format_tables(chunk)

        print(f"Received chunk: {chunk}\n")

    consumer.close()

consume_dataset()

"""
conn_params = {
    "host": "localhost",
    "database": "ny_taxi",
    "user": "root",
    "password": "root"
}
df = pd.DataFrame()

with psycopg2.connect(**conn_params) as conn, conn.cursor() as cursor:
    cursor.execute("SELECT * FROM yellow_taxi_data LIMIT 1 OFFSET 0;")
    column_names = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(columns=column_names)

    total_rows = cursor.fetchone()[0]

    start_row = 0
    batch_size = 5000

    while start_row < total_rows:
        query = f"SELECT * FROM yellow_taxi_data LIMIT {batch_size} OFFSET {start_row};"
        cursor.execute(query)
        data = cursor.fetchall()

        new_df = pd.DataFrame(data, columns=column_names)
        df = pd.concat([df, new_df], ignore_index=True)

        start_row += batch_size

datetime_dim = df[
        ['tpep_pickup_datetime','tpep_dropoff_datetime']
    ].drop_duplicates().reset_index(drop=True)

datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

datetime_dim['datetime_id'] = datetime_dim.index
datetime_dim = datetime_dim[
    ['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month',
     'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day',
     'drop_month', 'drop_year', 'drop_weekday']
]

passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}
rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
rate_code_dim['rate_code_id'] = rate_code_dim.index
rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
}
payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
payment_type_dim['payment_type_id'] = payment_type_dim.index
payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

df_locations = pd.read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')

pickup_location_dim = pd.DataFrame()
pickup_location_dim['LocationID'] = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
pickup_location_dim = pd.merge(pickup_location_dim, df_locations, on='LocationID')
pickup_location_dim = pickup_location_dim.rename(columns={'LocationID':'PULocationID'})

dropoff_location_dim = pd.DataFrame()
dropoff_location_dim['LocationID'] = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)
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
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']]
"""