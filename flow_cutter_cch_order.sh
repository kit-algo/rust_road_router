#!/usr/bin/env sh

flow_cutter_console \
  load_routingkit_unweighted_graph "$1/first_out" "$1/head" \
  add_back_arcs \
  reorder_nodes_in_flow_cutter_cch_order \
  save_routingkit_node_permutation_since_last_load "$1/cch_perm" \
