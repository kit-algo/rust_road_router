#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        rphast::{RPHASTQuery, RPHAST},
    },
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

    // for target_set_size_exp in [10, 12, 14] {
    for target_set_size_exp in [14] {
        for ball_size_exp in 14..=24 {
            let _exp_ctx = exps_ctxt.push_collection_item();

            report!("experiment", "rphast");
            report!("target_set_size_exp", target_set_size_exp);
            report!("ball_size_exp", ball_size_exp);

            let mut rng = experiments::rng(Default::default());

            let queries = experiments::gen_many_to_many_queries(&graph, num_queries, 2usize.pow(ball_size_exp), 2usize.pow(target_set_size_exp), &mut rng);

            let mut algos_ctxt = push_collection_context("algo_runs".to_string());
            let mut rphast = RPHAST::new(chpot_data.backward_graph(), chpot_data.forward_graph(), chpot_data.order().clone());
            let mut rphast_query = RPHASTQuery::new(&rphast);

            let mut total_query_time = Duration::zero();
            let mut total_selection_time = Duration::zero();

            for (sources, targets) in &queries {
                let (_, sel_time) = measure(|| {
                    rphast.select(&sources);
                });
                total_selection_time = total_selection_time + sel_time;
                for &target in targets.choose_multiple(&mut rng, num_queries) {
                    let _alg_ctx = algos_ctxt.push_collection_item();
                    report!("algo", "rphast_query");
                    let (_rphast_result, time) = measure(|| rphast_query.query(target, &rphast));
                    report!("selection_running_time_ms", sel_time.to_std().unwrap().as_secs_f64() * 1000.0);
                    report!("query_running_time_ms", time.to_std().unwrap().as_secs_f64() * 1000.0);
                    report!("running_time_ms", (time + sel_time).to_std().unwrap().as_secs_f64() * 1000.0);
                    total_query_time = total_query_time + time;
                }
            }

            if num_queries > 0 {
                eprintln!(
                    "Avg. selection time {}ms",
                    (total_selection_time.to_std().unwrap() / num_queries as u32).as_secs_f64() * 1000.0
                );
                eprintln!(
                    "Avg. query time {}ms",
                    (total_query_time.to_std().unwrap() / ((num_queries * num_queries) as u32)).as_secs_f64() * 1000.0
                );
            };
        }
    }

    Ok(())
}
