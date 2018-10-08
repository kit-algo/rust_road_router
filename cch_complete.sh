#!/usr/bin/env sh

if [ ! -f /data/first_out ]; then
  echo "importing here data from /import"
  ls -l /import
  cargo run --release -p bmw_routing_engine --bin import_here -- /import /data || { echo 'here import failed' ; exit 1; }
  mv /data/weights /data/travel_time
fi

if [ ! -f /data/cch_perm ]; then
  echo "calculating nested dissection order with flow cutter - might take a while"
  ./flow_cutter_cch_order.sh /data || { echo 'calculating nested dissection order with flowcutter failed' ; exit 1; }
fi

cd server && exec cargo run --release --bin bmw_routing_server -- /data
