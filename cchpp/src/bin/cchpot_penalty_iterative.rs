#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{ch_potentials::*, customizable_contraction_hierarchy::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;

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

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let mut penalty_server = {
        let _prepro_ctxt = algo_runs_ctxt.push_collection_item();
        penalty::Penalty::new(&graph, cch_pot.forward_potential(), cch_pot.backward_potential())
    };

    let mut total_query_time = std::time::Duration::ZERO;
    // let num_queries = experiments::chpot::num_queries();
    let num_queries = 10;

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);

        report!("from", from);
        report!("to", to);

        let (_res, time) = measure(|| penalty_server.alternatives_iterative(Query { from, to }));
        report!("running_time_ms", time.as_secs_f64() * 1000.0);
        report!(
            "pot_evals",
            penalty_server.potentials().map(|p| p.inner().inner().num_pot_computations()).sum::<usize>()
        );

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Avg. query time {}ms", (total_query_time / num_queries as u32).as_secs_f64() * 1000.0);
    };

    Ok(())
}
