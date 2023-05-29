"""ETL Flow to extract taxi data and upload into database"""
from argparse import ArgumentParser
from time import time
from sqlalchemy import create_engine

import pandas as pd


def main(params:ArgumentParser) -> None:
    """Main flow of extraction
    DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

    Args:
        params (ArgumentParser): Arguments for cli / docker
    """
    user     =  params.u
    password = params.pw
    host     =  params.h
    port     =  params.p
    database = params.db
    table    =  params.t
    url      =  params.U

    BATCH_SIZE = 10000
    DB_URL = f'postgresql://{user}:{password}@{host}:{port}/{database}'
#    DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

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

if __name__ == '__main__':
    parser = ArgumentParser(description='Taxi data here')

    parser.add_argument('--u', help='username for database')
    parser.add_argument('--pw', help='password for database')
    parser.add_argument('--h', help='host name for database')
    parser.add_argument('--p', help='port to acess database')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--t', help='table name in database')
    parser.add_argument('--U', help='url link to acess data file')

    main(parser.parse_args())
