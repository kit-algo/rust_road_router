#!/usr/bin/env sh

seed=5489

./lib/FlowCutter-Accelerated/release/console \
  load_routingkit_unweighted_graph "$1/first_out" "$1/head" \
  load_routingkit_longitude "$1/longitude" \
  load_routingkit_latitude "$1/latitude" \
  remove_multi_arcs \
  remove_loops \
  add_back_arcs \
  sort_arcs \
  flow_cutter_set random_seed $seed \
  reorder_nodes_at_random \
  reorder_nodes_in_preorder \
  flow_cutter_set thread_count ${2:-1} \
  flow_cutter_set BulkDistance no \
  flow_cutter_set max_cut_size 100000000 \
  flow_cutter_set distance_ordering_cutter_count 0 \
  flow_cutter_set geo_pos_ordering_cutter_count 8 \
  flow_cutter_set bulk_assimilation_threshold 0.4 \
  flow_cutter_set bulk_assimilation_order_threshold 0.25 \
  flow_cutter_set bulk_step_fraction 0.05 \
  flow_cutter_set initial_assimilated_fraction 0.05 \
  flow_cutter_set thread_count 2 \
  flow_cutter_config \
  report_time \
  reorder_nodes_in_accelerated_flow_cutter_cch_order \
  do_not_report_time \
  examine_chordal_supergraph \
  save_routingkit_node_permutation_since_last_load "$1/cch_perm" \
