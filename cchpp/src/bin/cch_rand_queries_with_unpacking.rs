use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        customizable_contraction_hierarchy::{query::Server, *},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_rand_queries_with_unpacking");
    report!("num_threads", rayon::current_num_threads());
    report!("perfect_customization", cfg!(feature = "perfect-customization"));
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
    let order = NodeOrder::from_node_order(Vec::load_from(path.join(args.next().unwrap_or("cch_perm".to_string())))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let customized = customize(&cch, &graph);

    #[cfg(feature = "perfect-customization")]
    let customized = customize_perfect(customized);

    let mut server = Server::new(customized);

    let mut algo_runs_ctxt = push_collection_context("queries");
    let num_queries = 1000000;
    for _ in 0..num_queries {
        let from = rng.gen_range(0..num_nodes as NodeId);
        let to = rng.gen_range(0..num_nodes as NodeId);
        let _query_ctxt = algo_runs_ctxt.push_collection_item();

        report!("from", from);
        report!("to", to);

        let mut res = silent_report_time(|| server.query_no_inline(Query { from, to }));
        let path = silent_report_time_with_key(
            "unpacking",
            #[inline(never)]
            || res.node_path(),
        );
        report!("num_nodes_in_searchspace", res.data().num_nodes_in_searchspace());
        report!("num_relaxed_edges", res.data().num_relaxed_edges());
        if let Some(path) = path {
            report!("num_nodes_on_path", path.len());
        }
    }

    Ok(())
}
