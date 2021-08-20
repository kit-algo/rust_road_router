#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{a_star::*, ch_potentials::*},
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("lazy_rphast");
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
            let mut many_to_one = chpot_data.potentials().0;

            let mut total_query_time = Duration::zero();

            for (sources, targets) in &queries {
                for &target in targets.choose_multiple(&mut rng, num_queries) {
                    let _alg_ctx = algos_ctxt.push_collection_item();
                    report!("algo", "lazy_rphast_many_to_one");
                    let (_, time) = measure(|| {
                        silent_report_time_with_key("selection", || {
                            many_to_one.init(target);
                        });
                        let mut queries_ctxt = push_collection_context("queries".to_string());
                        for &s in sources {
                            let _alg_ctx = queries_ctxt.push_collection_item();
                            silent_report_time(|| {
                                many_to_one.potential(s);
                            });
                        }
                    });
                    report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                    total_query_time = total_query_time + time;
                }
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}", total_query_time / ((num_queries * num_queries) as i32))
            };

            let mut many_to_many = chpot_data.bucket_ch_pot();
            let mut total_query_time = Duration::zero();

            for (sources, targets) in &queries {
                let _alg_ctx = algos_ctxt.push_collection_item();
                report!("algo", "lazy_rphast_many_to_many");
                let (_, time) = measure(|| {
                    report_time_with_key("selection", "selection", || {
                        many_to_many.init(&targets);
                    });
                    let mut queries_ctxt = push_collection_context("queries".to_string());

                    for &s in sources {
                        silent_report_time(|| {
                            let _alg_ctx = queries_ctxt.push_collection_item();
                            many_to_many.potential(s);
                        });
                    }
                });
                report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
            };
        }
    }

    Ok(())
}
