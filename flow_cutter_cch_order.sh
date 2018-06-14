#!/usr/bin/env sh

./lib/flow-cutter/console \
  load_routingkit_unweighted_graph "$1/first_out" "$1/head" \
  remove_multi_arcs \
  remove_loops \
  add_back_arcs \
  sort_arcs \
  reorder_nodes_in_flow_cutter_cch_order \
  save_routingkit_node_permutation_since_last_load "$1/cch_perm"
