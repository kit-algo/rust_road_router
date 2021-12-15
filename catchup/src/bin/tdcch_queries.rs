// Ad hoc CATCHUp query experiments for quick results.
// The real experiments use pregenerated query sets.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{catchup::Server, dijkstra::query::floating_td_dijkstra::Server as DijkServer, *},
    datastr::graph::{floating_time_dependent::*, *},
    experiments::catchup::setup,
    report::*,
};

use rand::prelude::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    // let _reporter = enable_reporting();
    report!("program", "tdcch_queries");

    setup(Path::new(&env::args().skip(1).next().unwrap()), |g, rng, cch, td_cch_graph| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());
        let mut td_dijk_server = DijkServer::new(g.clone());
        let mut server = Server::new(&cch, &td_cch_graph);

        let mut rank_times = vec![Vec::new(); 64];

        for _ in 0..50 {
            let from: NodeId = rng.gen_range(0..g.num_nodes() as NodeId);
            let at = Timestamp::new(rng.gen_range(0.0..f64::from(period())));
            td_dijk_server.ranks(from, at, |to, ea_ground_truth, rank| {
                let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
                let (mut result, duration) = measure(|| server.td_query(TDQuery { from, to, departure: at }).found().unwrap());

                report!("from", from);
                report!("to", to);
                report!("departure_time", f64::from(at));
                report!("rank", rank);
                report!("ground_truth", f64::from(ea_ground_truth));
                report!("running_time_ms", duration.as_secs_f64() * 1000.0);

                let ea = at + result.distance();
                report!("earliest_arrival", f64::from(ea));

                assert!(ea_ground_truth.fuzzy_eq(ea), "{} {} {:?}", from, to, at);

                let (path, unpacking_duration) = measure(|| result.node_path());
                report!("unpacking_running_time_ms", unpacking_duration.as_secs_f64() * 1000.0);
                rank_times[rank].push((duration, unpacking_duration));
                g.check_path(&path);
            });
        }

        for (rank, rank_times) in rank_times.into_iter().enumerate() {
            let count = rank_times.len();
            if count > 0 {
                let (sum, sum_unpacking) = rank_times
                    .into_iter()
                    .fold((Duration::ZERO, Duration::ZERO), |(acc1, acc2), (t1, t2)| (acc1 + t1, acc2 + t2));
                let avg = sum / count as u32;
                let avg_unpacking = sum_unpacking / count as u32;
                eprintln!(
                    "rank: {} - avg running time: {}ms - avg unpacking time: {}ms",
                    rank,
                    avg.as_secs_f64() * 1000.0,
                    avg_unpacking.as_secs_f64() * 1000.0
                );
            }
        }

        let num_queries = 50;

        let mut dijkstra_time = Duration::ZERO;
        let mut tdcch_time = Duration::ZERO;

        for _ in 0..num_queries {
            let from: NodeId = rng.gen_range(0..g.num_nodes() as NodeId);
            let to: NodeId = rng.gen_range(0..g.num_nodes() as NodeId);
            let at: u32 = rng.gen_range(0..86400000);
            let at = Timestamp::new(f64::from(at) / 1000.0);

            let dijkstra_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (ground_truth, time) = measure(|| td_dijk_server.td_query(TDQuery { from, to, departure: at }).distance().map(|d| d + at));
            report!("from", from);
            report!("to", to);
            report!("departure_time", f64::from(at));
            report!("running_time_ms", time.as_secs_f64() * 1000.0);
            if let Some(ground_truth) = ground_truth {
                report!("earliest_arrival", f64::from(ground_truth));
            }
            drop(dijkstra_query_ctxt);

            dijkstra_time = dijkstra_time + time;

            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (result, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }));
            tdcch_time = tdcch_time + time;

            report!("from", from);
            report!("to", to);
            report!("departure_time", f64::from(at));
            report!("running_time_ms", time.as_secs_f64() * 1000.0);
            if let Some(ground_truth) = ground_truth {
                report!("ground_truth", f64::from(ground_truth));
            }
            let ea = if let Some(dist) = result.distance() {
                report!("earliest_arrival", f64::from(dist + at));
                dist + at
            } else {
                Timestamp::NEVER
            };

            assert!(ea.fuzzy_eq(ground_truth.unwrap_or(Timestamp::NEVER)));
        }
        eprintln!("Dijkstra {}ms", (dijkstra_time / num_queries).as_secs_f64() * 1000.0);
        eprintln!("TDCCH {}ms", (tdcch_time / num_queries).as_secs_f64() * 1000.0);
        Ok(())
    })
}
