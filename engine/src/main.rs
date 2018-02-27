use std::env;
use std::path::Path;
use std::sync::Arc;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use graph::*;
use graph::first_out_graph::OwnedGraph as Graph;
use shortest_path::query::dijkstra::Server as DijkServer;
use shortest_path::query::bidirectional_dijkstra::Server as BiDijkServer;
use shortest_path::query::async::dijkstra::Server as AsyncDijkServer;
use shortest_path::query::async::bidirectional_dijkstra::Server as AsyncBiDijkServer;
use shortest_path::query::contraction_hierarchy::Server as CHServer;
use shortest_path::contraction_hierarchy;

use io::Load;
use bmw_routing_engine::benchmark::measure;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Arc::new(Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out"));
    let head = Arc::new(Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head"));
    let travel_time = Arc::new(Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time"));

    let from = Vec::load_from(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = Vec::load_from(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let arcd_graph = FirstOutGraph::new(first_out.clone(), head.clone(), travel_time.clone());
    let mut simple_server = DijkServer::new(graph.clone());
    let mut bi_dir_server = BiDijkServer::new(graph.clone());
    let async_server = AsyncDijkServer::new(arcd_graph.clone());
    let mut async_bi_dir_server = AsyncBiDijkServer::new(arcd_graph);

    let ch_first_out = Vec::load_from(path.join("travel_time_ch/first_out").to_str().unwrap()).expect("could not read travel_time_ch/first_out");
    let ch_head = Vec::load_from(path.join("travel_time_ch/head").to_str().unwrap()).expect("could not read travel_time_ch/head");
    let ch_weight = Vec::load_from(path.join("travel_time_ch/weight").to_str().unwrap()).expect("could not read travel_time_ch/weight");
    let ch_order = Vec::load_from(path.join("travel_time_ch/order").to_str().unwrap()).expect("could not read travel_time_ch/order");
    let mut inverted_order = vec![0; ch_order.len()];
    for (i, &node) in ch_order.iter().enumerate() {
        inverted_order[node as usize] = i as u32;
    }
    let mut ch_server = CHServer::new((Graph::new(ch_first_out, ch_head, ch_weight).ch_split(&inverted_order), None));
    let mut ch_server_with_own_ch = CHServer::new(contraction_hierarchy::contract(graph, ch_order));

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        measure("simple dijkstra", || {
            assert_eq!(simple_server.distance(from, to), ground_truth);
        });
        measure("bidir dijkstra", || {
            assert_eq!(bi_dir_server.distance(from, to), ground_truth);
        });
        measure("async dijkstra", || {
            assert_eq!(async_server.distance(from, to), ground_truth);
        });
        measure("async bidir dijkstra", || {
            assert_eq!(async_bi_dir_server.distance(from, to), ground_truth);
        });
        measure("CH", || {
            assert_eq!(ch_server.distance(from, to), ground_truth);
        });
        measure("own CH", || {
            assert_eq!(ch_server_with_own_ch.distance(inverted_order[from as usize], inverted_order[to as usize]), ground_truth);
        });
    }
}
