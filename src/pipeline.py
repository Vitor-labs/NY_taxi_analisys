"""
Module Docstring
"""
from argparse import ArgumentParser
from .stages.extract import download_dataset

class Pipeline:
    """
    Main class to run the pipeline
    """
    @classmethod
    def run(cls, params: ArgumentParser) -> None:
        args = {}

        args['user']     =  params.u
        args['password'] = params.pw
        args['host']     =  params.h
        args['port']     =  params.p
        args['database'] = params.db
        args['table']    =  params.t
        args['url']      =  params.U

        download_dataset()


if __name__ == '__main__':
    parser = ArgumentParser(description='Taxi data here')

    parser.add_argument('--u', help='username for database')
    parser.add_argument('--pw', help='password for database')
    parser.add_argument('--h', help='host name for database')
    parser.add_argument('--p', help='port to acess database')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--t', help='table name in database')
    parser.add_argument('--U', help='url link to acess data file')

    Pipeline.run(parser.parse_args())
