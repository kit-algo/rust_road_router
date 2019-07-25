use std::env;
use std::path::Path;

use bmw_routing_engine::{
    graph::*,
    shortest_path::{
        customizable_contraction_hierarchy,
        node_order::NodeOrder,
        query::customizable_contraction_hierarchy::Server,
    },
    io::Load,
    benchmark::report_time,
};

fn main() {
    let core_ids = core_affinity::get_core_ids().unwrap();
    rayon::ThreadPoolBuilder::new().start_handler(move |thread_idx| core_affinity::set_for_current(core_ids[thread_idx])).build_global().unwrap();

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order.clone());
    let cch_order = customizable_contraction_hierarchy::cch_graph::CCHReordering { node_order: cch_order, latitude: &[], longitude: &[] }.reorder_for_seperator_based_customization(cch.separators());
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order);

    let mut server = Server::new(&cch, &graph);

    let from = Vec::load_from(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = Vec::load_from(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    report_time("10000 CCH queries", || {
        for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(10000) {
            let ground_truth = match ground_truth {
                INFINITY => None,
                val => Some(val),
            };

            assert_eq!(server.distance(from, to), ground_truth);
            server.path();
        }
    });
}
