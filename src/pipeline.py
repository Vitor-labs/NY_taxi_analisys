"""
Module Docstring
"""
from argparse import ArgumentParser

import hvac

from stages.extract import download_dataset
from stages.transform import consume_dataset#, process_dataset

class Pipeline:
    """
    Main class to run the pipeline
    """
    @classmethod
    def run(cls) -> None:
        """
        Main Process
        """
        print("Running main pipeline")


if __name__ == '__main__':
    parser = ArgumentParser(description='Taxi data here')

    parser.add_argument('--u', help='username for database')
    parser.add_argument('--pw', help='password for database')
    parser.add_argument('--h', help='host name for database')
    parser.add_argument('--p', help='port to acess database')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--t', help='table name in database')
    parser.add_argument('--U', help='url link to acess data file')
    parser.add_argument('--v', help='IP for vault server')
    #python pipeline.py --u=root --pw=root --h=pg-database --p=5432 --db=ny_taxi --t=yellow_taxi_data --U=${URL} --v=http://127.0.0.1:8200

    client = hvac.Client(url='http://localhost:8200')
    if client.is_authenticated():
        client.secrets.kv.v2.create_or_update_secret(path='user', secret=dict(sec=parser.u))
        client.secrets.kv.v2.create_or_update_secret(path='password', secret=dict(sec=parser.pw))
        client.secrets.kv.v2.create_or_update_secret(path='host', secret=dict(sec=parser.h))
        client.secrets.kv.v2.create_or_update_secret(path='port', secret=dict(sec=parser.p))
        client.secrets.kv.v2.create_or_update_secret(path='database', secret=dict(sec=parser.db))
        client.secrets.kv.v2.create_or_update_secret(path='table', secret=dict(sec=parser.t))
        client.secrets.kv.v2.create_or_update_secret(path='url', secret=dict(sec=parser.U))
        client.secrets.kv.v2.create_or_update_secret(path='vault', secret=dict(sec=parser.v))
        
        print('Secrets Added')

        Pipeline.run()
    else:
        raise Exception('Vault not authentificated')  
