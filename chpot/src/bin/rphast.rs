#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{ch_potentials::*, rphast::RPHAST},
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("rphast");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;

    let num_queries = 100;

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    for target_set_size_exp in [10, 12, 14] {
        for ball_size_exp in 14..=24 {
            let _exp_ctx = exps_ctxt.push_collection_item();

            report!("experiment", "lazy_rphast");
            report!("target_set_size_exp", target_set_size_exp);
            report!("ball_size_exp", ball_size_exp);

            let mut rng = experiments::rng(Default::default());

            let queries = experiments::gen_many_to_many_queries(&graph, num_queries, 2usize.pow(ball_size_exp), 2usize.pow(target_set_size_exp), &mut rng);

            let mut algos_ctxt = push_collection_context("algo_runs".to_string());
            let mut rphast = RPHAST::new(chpot_data.backward_graph(), chpot_data.forward_graph(), chpot_data.order().clone());

            let mut total_query_time = Duration::zero();
            let mut total_selection_time = Duration::zero();

            for (sources, targets) in &queries {
                {
                    let _alg_ctx = algos_ctxt.push_collection_item();
                    report!("algo", "rphast_selection");
                    let (_, time) = measure(|| {
                        rphast.select(&sources);
                    });
                    report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                    total_selection_time = total_selection_time + time;
                }
                for &target in targets.choose_multiple(&mut rng, num_queries) {
                    let _alg_ctx = algos_ctxt.push_collection_item();
                    report!("algo", "rphast_query");
                    let (_rphast_result, time) = measure(|| rphast.query(target));
                    report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                    total_query_time = total_query_time + time;
                }
            }

            if num_queries > 0 {
                eprintln!("Avg. selection time {}", total_selection_time / num_queries as i32);
                eprintln!("Avg. query time {}", total_query_time / ((num_queries * num_queries) as i32));
            };
        }
    }

    Ok(())
}
