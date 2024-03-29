// CATCHUp query experiments with pregenerated queries with source and target drawn uniformly at random.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        catchup::{profiles::Server, Server as EAServer},
        *,
    },
    datastr::graph::{floating_time_dependent::*, *},
    experiments::catchup::setup,
    report::*,
};

use rand::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_profiles");

    setup(Path::new(&env::args().skip(1).next().unwrap()), |g, rng, cch, td_cch_graph| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs");

        let mut server = Server::new(&cch, &td_cch_graph);
        let mut ea_server = EAServer::new(&cch, &td_cch_graph);

        let mut tdcch_time = std::time::Duration::ZERO;

        let n = g.num_nodes();

        for _ in 0..50 {
            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            eprintln!();
            let from: NodeId = rng.gen_range(0..n as NodeId);
            let to: NodeId = rng.gen_range(0..n as NodeId);
            report!("from", from);
            report!("to", to);

            let ((shortcut, tt, paths), time) = measure(|| server.distance(from, to));

            report!("running_time_ms", time.as_secs_f64() * 1000.0);
            tdcch_time = tdcch_time + time;
            report!("num_sources", shortcut.num_sources());
            if paths.is_empty() {
                report!("path_switches", 0usize);
                report!("num_distinct_paths", 0usize);
            } else {
                let mut check_segment = |start: Timestamp, end: Timestamp, path: &[EdgeId]| {
                    let _blocked = block_reporting();
                    let at = start + 0.5 * (end - start);
                    let mut result = ea_server.td_query(TDQuery { from, to, departure: at });
                    assert!(PeriodicPiecewiseLinearFunction::new(&tt).evaluate(at).fuzzy_eq(result.distance().unwrap()));
                    let gt_path = result.node_path().unwrap();
                    g.check_path(&gt_path);
                    g.check_path(&g.get_path_with_times(at, &path));
                };
                for paths in paths.windows(2) {
                    check_segment(paths[0].0, paths[1].0, &paths[0].1);
                }
                check_segment(paths.last().unwrap().0, period(), &paths.last().unwrap().1);

                report!("path_switches", paths.len() - 1);
                let mut paths: Vec<_> = paths.into_iter().map(|(_, path)| path).collect();
                paths.sort();
                paths.dedup();
                report!("num_distinct_paths", paths.len());
            }
        }

        eprintln!("Avg CATCHUp Profile Time {}s", (tdcch_time / 50).as_secs_f64());

        Ok(())
    })
}
