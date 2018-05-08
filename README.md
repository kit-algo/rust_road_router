# Running CCH with Docker

```
git submodule update --init --recursive
docker build -t routing_engine .
docker run -p <target port>:80 --mount 'type=bind,src=<folder containing here csvs>,dst=/import' --mount 'type=volume,src=routing_engine_data,dst=/data' routing_engine
```

Here import will take 10 - 20 minutes and might need a lot of RAM for the link geometry.
FlowCutter some hours.
Actual CCH preprocessing around 10 minutes
