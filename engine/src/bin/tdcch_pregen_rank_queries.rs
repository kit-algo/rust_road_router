// CATCHUp query experiments with pregenerated rank queries.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{catchup::Server, *},
    cli::CliErr,
    datastr::graph::floating_time_dependent::*,
    experiments::catchup::setup,
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_pregen_rank_queries");
    let arg = env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    setup(Path::new(&env::args().skip(1).next().unwrap()), |_g, rng, cch, td_cch_graph| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());
        let mut server = Server::new(&cch, &td_cch_graph);

        let mut query_dir = None;
        let mut base_dir = Some(path);

        while let Some(base) = base_dir {
            if base.join("rank_queries").exists() {
                query_dir = Some(base.join("rank_queries"));
                break;
            } else {
                base_dir = base.parent();
            }
        }

        if let Some(path) = query_dir {
            let from = Vec::load_from(path.join("source_node"))?;
            let at = Vec::<u32>::load_from(path.join("source_time"))?;
            let to = Vec::load_from(path.join("target_node"))?;
            let rank = Vec::<u32>::load_from(path.join("dij_rank"))?;

            for q_idx in rand::seq::index::sample(rng, from.len(), from.len()).into_iter() {
                let from = from[q_idx];
                let to = to[q_idx];
                let at = Timestamp::new(f64::from(at[q_idx]) / 1000.0);

                let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
                let (result, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }));

                report!("from", from);
                report!("to", to);
                report!("rank", rank[q_idx]);
                report!("departure_time", f64::from(at));
                report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                if let Some(mut result) = result {
                    report!("earliest_arrival", f64::from(result.distance() + at));

                    let (path, unpacking_duration) = measure(|| result.path());
                    report!("num_nodes_on_shortest_path", path.len());
                    report!(
                        "unpacking_running_time_ms",
                        unpacking_duration.to_std().unwrap().as_nanos() as f64 / 1_000_000.0
                    );
                }
            }
        }

        Ok(())
    })
}
