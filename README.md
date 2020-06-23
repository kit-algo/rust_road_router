# Rust routing framework and toolkit

There are two crates.
The engine which is a library containing several routing algorithms and utilities for working with routing data.
The server crate builds on the engine library and provides and HTTP interface for answering routing queries.
Refer to the readmes of the respective crates for more information.

Additonally, there is a `lib` directory, which contains `InertialFlowCutter`, a partitioning program to calculate nested disection orders for CCHs, as a git submodule.


# Running CCH server with Docker

Using the following commands, a docker container can be launched, which imports routing data from HERE CSV files and runs the routing server.

```
git submodule update --init --recursive
docker build -t routing_engine .
docker run -p <target port>:80 --mount 'type=bind,src=<folder containing here csvs>,dst=/import' --mount 'type=volume,src=routing_engine_data,dst=/data' routing_engine
```

Here import will take 10 - 20 minutes and might need a lot of RAM for the link geometry.
FlowCutter some minutes.
Actual CCH preprocessing less than a minute.
Refer to the readme of the server crate for API documentation.
