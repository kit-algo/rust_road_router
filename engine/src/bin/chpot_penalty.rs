#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{a_star::RecyclingPotential, ch_potentials::*, *},
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_penalty");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut rng = experiments::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;
    let (forward_pot, backward_pot) = chpot_data.potentials();
    let (forward_pot, backward_pot) = (RecyclingPotential::new(forward_pot), RecyclingPotential::new(backward_pot));

    let mut penalty_server = {
        let _prepro_ctxt = algo_runs_ctxt.push_collection_item();
        penalty::Penalty::new(&graph, forward_pot)
    };

    let mut total_query_time = Duration::zero();
    let num_queries = experiments::chpot::num_queries();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0..graph.num_nodes() as NodeId);

        eprintln!();
        report!("from", from);
        report!("to", to);

        let (_, time) = measure(|| penalty_server.alternatives(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("pot_evals", penalty_server.potential().inner().num_pot_computations());
        eprintln!();

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
    };

    Ok(())
}
