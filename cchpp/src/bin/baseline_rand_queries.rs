use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::dijkstra::{query::dijkstra::Server, DefaultOps},
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_rand_queries_by_features");
    let mut rng = experiments::rng(Default::default());
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let weight_file = args.next().unwrap_or("travel_time".to_string());
    report!("weight_file", weight_file);
    let weight = Vec::<Weight>::load_from(path.join(weight_file))?;
    let topology = UnweightedOwnedGraph::reconstruct_from(&path)?;
    let graph = FirstOutGraph::new(topology.first_out(), topology.head(), &weight[..]);

    let mut server = Server::<_, DefaultOps, _>::new(graph.clone());

    let mut _query_ctxt = push_collection_context("queries");
    experiments::run_random_queries(graph.num_nodes(), &mut server, &mut rng, &mut _query_ctxt, 1000);

    Ok(())
}
