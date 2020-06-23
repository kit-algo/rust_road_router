#!/usr/bin/env sh

# Run the complete cch toolchain, including importing here data.
# File locations assume that this is run within the Docker container.

if [ ! -f /data/first_out ]; then
  echo "importing here data from /import"
  ls -l /import
  cargo run --release -p rust_road_router --bin import_here -- /import /data || { echo 'here import failed' ; exit 1; }
  mv /data/weights /data/travel_time
fi

if [ ! -f /data/cch_perm ]; then
  echo "calculating nested dissection order with InertialFlowCutter - might take a a couple of minutes"
  ./flow_cutter_cch_order.sh /data $(nproc --all) || { echo 'calculating nested dissection order with flowcutter failed' ; exit 1; }
fi

cd server && exec cargo run --release --bin server -- /data
