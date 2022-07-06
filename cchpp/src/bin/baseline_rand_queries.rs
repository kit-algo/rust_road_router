use rand::prelude::*;
use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        dijkstra::{query::dijkstra::Server, DefaultOps},
        Query, QueryServer,
    },
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
    let num_nodes = graph.num_nodes();

    let mut server = Server::<_, DefaultOps, _>::new(graph);

    let mut _queries_ctxt = push_collection_context("queries");
    let num_queries = 1000;
    for _ in 0..num_queries {
        let from = rng.gen_range(0..num_nodes as NodeId);
        let to = rng.gen_range(0..num_nodes as NodeId);
        let _query_ctxt = _queries_ctxt.push_collection_item();

        report!("from", from);
        report!("to", to);

        let mut res = silent_report_time(|| server.query_no_inline(Query { from, to }));
        let path = silent_report_time_with_key(
            "unpacking",
            #[inline(never)]
            || res.node_path(),
        );
        if let Some(path) = path {
            report!("num_nodes_on_path", path.len());
        }
    }

    Ok(())
}
