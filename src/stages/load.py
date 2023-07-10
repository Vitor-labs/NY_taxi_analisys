from typing import Dict
from time import time
from sqlalchemy import create_engine

import psycopg2

def main(params: Dict) -> None:
    user     =  params['user']
    password = params['password']
    host     =  params['host']
    port     =  params['port']
    database = params['database']
    table    =  params['table']
    url      =  params['url']
    
    DB_URL = f'postgresql://{user}:{password}@{host}:{port}/{database}'

    trips = pd.read_parquet(url)

    engine = create_engine(DB_URL)
    engine.connect()

    print(pd.io.sql.get_schema(trips, name=table, con=engine))

    trips.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

    total_rows = len(trips)
    num_batches = total_rows // BATCH_SIZE + 1

    print("Inserting chuncks...")
    for i in range(num_batches):
        t_start = time()
        start = i * BATCH_SIZE
        end = min((i + 1) * BATCH_SIZE, total_rows)

        batch_data = trips.iloc[start:end]

        batch_data.to_sql(name=table, con=engine, if_exists='append')
        t_end = time()

        print(f'...Inserted {end} lines, took {t_end - t_start}s')

