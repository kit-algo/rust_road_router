#!/usr/bin/env sh

./lib/flow-cutter/console \
  flow_cutter_set thread_count ${2:-1} \
  load_routingkit_unweighted_graph "$1/first_out" "$1/head" \
  report_time \
  remove_multi_arcs \
  remove_loops \
  add_back_arcs \
  sort_arcs \
  reorder_nodes_in_flow_cutter_cch_order \
  do_not_report_time \
  save_routingkit_node_permutation_since_last_load "$1/cch_perm"
