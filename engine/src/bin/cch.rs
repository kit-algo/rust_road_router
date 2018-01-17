use std::env;
use std::path::Path;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use graph::first_out_graph::FirstOutGraph as Graph;
use graph::INFINITY;
use shortest_path::customizable_contraction_hierarchy;
use shortest_path::node_order::NodeOrder;
use shortest_path::query::customizable_contraction_hierarchy::Server;
use io::read_into_vector;
use bmw_routing_engine::benchmark::measure;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let from = read_into_vector(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = read_into_vector(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = read_into_vector(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = Graph::new(first_out, head, travel_time);
    let cch_order = read_into_vector(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

    let mut cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));
    cch.customize(&graph);

    let mut server = Server::new(cch);

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        measure("CCH query", || {
            assert_eq!(server.distance(from, to), ground_truth);
        });
    }
}