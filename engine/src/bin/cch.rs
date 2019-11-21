use std::{env, error::Error, path::Path};

use bmw_routing_engine::{
    benchmark::report_time,
    cli::CliErr,
    graph::*,
    io::Load,
    shortest_path::{customizable_contraction_hierarchy::*, node_order::NodeOrder, query::customizable_contraction_hierarchy::Server},
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap())?;
    let head = Vec::load_from(path.join("head").to_str().unwrap())?;
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap())?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap())?;
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order.clone());
    let cch_order = CCHReordering {
        node_order: cch_order,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization(cch.separators());
    let cch = contract(&graph, cch_order);

    let mut server = Server::new(&cch, &graph);

    let from = Vec::load_from(path.join("test/source").to_str().unwrap())?;
    let to = Vec::load_from(path.join("test/target").to_str().unwrap())?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap())?;

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

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

    Ok(())
}
