use std::env;
use std::path::Path;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use graph::*;
use shortest_path::customizable_contraction_hierarchy;
use shortest_path::node_order::NodeOrder;
use shortest_path::query::customizable_contraction_hierarchy::Server;
use io::Load;
use bmw_routing_engine::benchmark::measure;
use shortest_path::query::dijkstra::Server as DijkServer;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let from = Vec::load_from(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = Vec::load_from(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

    let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));

    let mut server = Server::new(&cch, &graph);
    let mut simple_server = DijkServer::new(graph);

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        measure("CCH query", || {
            assert_eq!(server.distance(from, to), ground_truth);
        });

        measure("Dijkstra query", || {
            assert_eq!(simple_server.distance(from, to), ground_truth);
        });

        assert_eq!(simple_server.path(), server.path());
    }
}
