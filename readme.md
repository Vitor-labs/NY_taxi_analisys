## how to run this project

1. Create a docker network
```sh
docker network create pg-network
```
2. Build the docker image
```sh
docker build -t <container_name> .
```
3. Run the image
```sh
docker run -it --network=pg-network data_ingest:v001 --u=root --pw=root --h=local/host --p=5432 --db=ny_taxi --t=yellow_taxi_data --U={url}
```