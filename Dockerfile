#python ny_taxi_data_etl.py --u=root --pw=root --h=local/host --p=5432 --db=ny_taxi --t=yellow_taxi_data --U=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
FROM python:3.10

RUN pip install pandas sqlalchemy kafka-python psycopg2 hvac

WORKDIR /app

COPY . /app

ENTRYPOINT [ "python", "pipeline.py" ]