// CATCHUp query experiments with pregenerated rank queries.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{algo::catchup::profiles::Server, cli::CliErr, experiments::catchup::setup, io::*, report::*};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_pregen_rank_queries");
    let arg = env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    setup(Path::new(&env::args().skip(1).next().unwrap()), |_g, rng, cch, td_cch_graph| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs");
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
            let to = Vec::load_from(path.join("target_node"))?;
            let rank = Vec::<u32>::load_from(path.join("dij_rank"))?;

            for q_idx in rand::seq::index::sample(rng, from.len(), 2400).into_iter() {
                let from = from[q_idx];
                let to = to[q_idx];

                let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
                report!("from", from);
                report!("to", to);
                report!("rank", rank[q_idx]);

                let ((_, _, paths), time) = measure(|| server.distance(from, to));

                if paths.is_empty() {
                    report!("path_switches", 0usize);
                    report!("num_distinct_paths", 0usize);
                } else {
                    report!("path_switches", paths.len() - 1);
                    let mut paths: Vec<_> = paths.into_iter().map(|(_, path)| path).collect();
                    paths.sort();
                    paths.dedup();
                    report!("num_distinct_paths", paths.len());
                }

                report!("running_time_ms", time.as_secs_f64() * 1000.0);
            }
        }

        Ok(())
    })
}
