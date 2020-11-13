// CATCHUp query experiments with pregenerated queries with source and target drawn uniformly at random.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{algo::catchup::profiles::Server, datastr::graph::*, experiments::catchup::setup, report::*};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();
    report!("program", "tdcch_profiles");

    setup(Path::new(&env::args().skip(1).next().unwrap()), |g, rng, cch, td_cch_graph| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

        let mut server = Server::new(&cch, &td_cch_graph);

        let mut tdcch_time = Duration::zero();

        let n = g.num_nodes();

        for _ in 0..10 {
            eprintln!();
            let from: NodeId = rng.gen_range(0, n as NodeId);
            let to: NodeId = rng.gen_range(0, n as NodeId);

            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (result, time) = measure(|| server.distance(from, to));

            report!("from", from);
            report!("to", to);
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            tdcch_time = tdcch_time + time;
            report!("num_sources", result.num_sources());
        }

        eprintln!("TDCCH {}", tdcch_time / (10i32));

        Ok(())
    })
}
