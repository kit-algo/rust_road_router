#[macro_use]
extern crate rust_road_router;

use rand::prelude::*;
use rust_road_router::{
    algo::{ch_potentials::*, customizable_contraction_hierarchy::*, traffic_aware::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::*,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("traffic_aware");
    let mut rng = rng(Default::default());
    let num_queries = chpot::num_queries();

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);
    let epsilon = args.next().map(|arg| arg.parse().expect("could not parse epsilon")).unwrap_or(1.0);
    report!("epsilon", epsilon);
    let live_weight_file = args.next().unwrap_or("live_travel_time".to_string());
    report!("live_weight_file", live_weight_file);
    let queries = args.next();
    report!("queries", queries.as_deref().unwrap_or("rand"));

    let mut graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    for w in graph.weights_mut() {
        *w = std::cmp::max(1, *w);
    }
    let n = graph.num_nodes();

    let sources = queries
        .as_ref()
        .map(|queries| Vec::<NodeId>::load_from(path.join(queries).join("source")).unwrap())
        .unwrap_or_else(|| std::iter::from_fn(|| Some(rng.gen_range(0..n as NodeId))).take(num_queries).collect());
    let targets = queries
        .as_ref()
        .map(|queries| Vec::<NodeId>::load_from(path.join(queries).join("target")).unwrap())
        .unwrap_or_else(|| std::iter::from_fn(|| Some(rng.gen_range(0..n as NodeId))).take(num_queries).collect());

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let smooth_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let mut modified_travel_time = Vec::<Weight>::load_from(path.join(live_weight_file))?;
    for (w, smooth) in modified_travel_time.iter_mut().zip(graph.weight().iter()) {
        if *w == 0 {
            *w = *smooth;
        }
    }
    let live_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);
    for &w in live_graph.weight() {
        assert!(w > 0);
    }

    let live_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &live_graph)
    };

    let num_queries = std::cmp::min(num_queries, sources.len());
    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let mut server = HeuristicTrafficAwareServer::new(graph.borrowed(), live_graph.clone(), &smooth_cch_pot, &live_cch_pot);

    let mut total_query_time = std::time::Duration::ZERO;

    for (&from, &to) in sources.iter().zip(targets.iter()).take(num_queries) {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();

        report!("from", from);
        report!("to", to);

        let (_, time) = measure(|| server.query(Query { from, to }, epsilon, |_| ()));
        report!("running_time_ms", time.as_secs_f64() * 1000.0);

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!(
            "Heuristic: Avg. query time {}ms",
            (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0
        )
    };

    let mut server = TrafficAwareServer::new(graph.borrowed(), live_graph, &smooth_cch_pot, &live_cch_pot);

    let mut total_query_time = std::time::Duration::ZERO;

    for (&from, &to) in sources.iter().zip(targets.iter()).take(num_queries) {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();

        report!("from", from);
        report!("to", to);

        let (_, time) = measure(|| server.query(Query { from, to }, epsilon));
        report!("running_time_ms", time.as_secs_f64() * 1000.0);

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Exact: Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
    };

    Ok(())
}
