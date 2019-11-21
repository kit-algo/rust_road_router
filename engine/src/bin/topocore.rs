use bmw_routing_engine::shortest_path::customizable_contraction_hierarchy::*;
use bmw_routing_engine::shortest_path::node_order::*;
use std::{env, path::Path};

use time::Duration;

use bmw_routing_engine::{
    benchmark::*,
    graph::*,
    io::Load,
    shortest_path::{
        query::topocore::Server as TopoServer,
        // query::dijkstra::Server as DijkServer,
        topocore::preprocess,
    },
};

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude").to_str().unwrap()).expect("could not read latitude");
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude").to_str().unwrap()).expect("could not read longitude");

    let from = Vec::load_from(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = Vec::load_from(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order.clone());
    let cch_order = CCHReordering {
        node_order: cch_order,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization(cch.separators());
    let cch = contract(&graph, cch_order);

    // let mut simple_server = DijkServer::new(graph);

    let topocore = report_time("topocore preprocessing", || preprocess(&graph));
    let mut topocore = TopoServer::new(topocore, &cch, &graph);

    let mut total_query_time = Duration::zero();

    let num_queries = 1000;

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(num_queries) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        let (res, time) = measure(|| {
            // simple_server.distance(from, to)
            #[cfg(feature = "chpot_visualize")]
            {
                topocore.distance(from, to, &lat, &lng)
            }
            #[cfg(not(feature = "chpot_visualize"))]
            {
                topocore.distance(from, to)
            }
        });
        topocore.path();
        if res != ground_truth {
            eprintln!("topo {:?} ground_truth {:?} ({} - {})", res, ground_truth, from, to);
        }
        // assert_eq!(res, ground_truth, "{} - {}", from, to);

        total_query_time = total_query_time + time;
    }

    eprintln!("Avg. query time {}", total_query_time / (num_queries as i32));
}
