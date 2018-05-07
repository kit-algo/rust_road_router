```
docker build -t routing_engine .
docker run -p 8888:80 --mount 'type=bind,src=<folder containing here csvs>,dst=/import' --mount 'type=volume,src=routing_engine_data,dst=/data' routing_engine
```
