#!/usr/bin/env sh

if [ ! -f /data/first_out ]; then
  echo "importing here data from /import"
  cargo run --release -p bmw_routing_engine --bin import_here -- /import /data
fi

if [ ! -f /data/cch_perm ]; then
  echo "calculating nested dissection order with flow cutter - might take a while"
  ./flow_cutter_cch_order.sh /data
fi

cd server && cargo run --release --bin bmw_routing_server -- /data
