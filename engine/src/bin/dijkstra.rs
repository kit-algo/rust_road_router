#[macro_use]
extern crate rust_road_router;
use rust_road_router::{algo::dijkstra::*, cli::CliErr, datastr::graph::*, experiments, io::*, report::*};
use std::{env, error::Error, path::Path};

pub fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("dijkstra");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut rng = experiments::rng(Default::default());

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;

    let graph = FirstOutGraph::new(first_out, head, travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let mut server = rust_road_router::algo::dijkstra::query::dijkstra::Server::<OwnedGraph, DefaultOps, _, &OwnedGraph>::new(&graph);

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    experiments::run_random_queries(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut &mut algo_runs_ctxt,
        rust_road_router::experiments::num_dijkstra_queries(),
    );

    Ok(())
}
