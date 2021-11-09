#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{ch_potentials::*, customizable_contraction_hierarchy::*, minimal_nonshortest_subpaths::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_penalty_iterative");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut rng = experiments::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let mut penalty_server = {
        let _prepro_ctxt = algo_runs_ctxt.push_collection_item();
        penalty::PenaltyIterative::new(&graph, &cch_pot)
    };
    let mut path_stats = MinimalNonShortestSubPaths::new(&cch_pot);

    let mut total_query_time = Duration::zero();
    let num_queries = experiments::chpot::num_queries();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);

        report!("from", from);
        report!("to", to);

        let (res, time) = measure(|| penalty_server.alternatives(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!(
            "pot_evals",
            penalty_server.potentials().map(|p| p.inner().inner().num_pot_computations()).sum::<usize>()
        );
        if let Some(alternative) = res {
            let (lo, ubs) = path_stats.suboptimal_stats(&alternative);
            report!("local_optimality_percent", lo * 100.0);
            report!("ubs", ubs);
        }

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
    };

    Ok(())
}
