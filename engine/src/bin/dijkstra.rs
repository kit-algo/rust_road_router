use rust_road_router::{algo::dijkstra::*, cli::CliErr, datastr::graph::*, experiments, io::*, report::*};
use std::{env, error::Error, path::Path};

pub fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("dijkstra");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut rng = experiments::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let mut server = rust_road_router::algo::dijkstra::query::dijkstra::Server::<OwnedGraph, DefaultOps, _, &OwnedGraph>::new(&graph);

    experiments::run_random_queries(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut &mut algo_runs_ctxt,
        rust_road_router::experiments::num_dijkstra_queries(),
    );

    Ok(())
}
