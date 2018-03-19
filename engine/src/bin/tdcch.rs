use std::env;
use std::path::Path;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use graph::time_dependent::*;
use shortest_path::customizable_contraction_hierarchy;
use shortest_path::node_order::NodeOrder;
use io::Load;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");
    let period = 86400000;

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time, period);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

    let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));
    let td_cch_graph = cch.customize_td(&graph);
    println!("{:?}", td_cch_graph.total_num_segments());
}
