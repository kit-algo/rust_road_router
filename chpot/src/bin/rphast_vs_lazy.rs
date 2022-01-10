#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{a_star::*, ch_potentials::*, rphast::*},
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::time::Duration;
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("lazy_rphast");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;

    let num_queries = 10;

    let mut exps_ctxt = push_collection_context("experiments");

    // for target_set_size_exp in [10, 12, 14] {
    for target_set_size_exp in [14] {
        // for ball_size_exp in 14..=24 {
        for ball_size_exp in 14..=14 {
            let _exp_ctx = exps_ctxt.push_collection_item();

            report!("experiment", "lazy_rphast");
            report!("target_set_size_exp", target_set_size_exp);
            report!("ball_size_exp", ball_size_exp);

            let mut rng = experiments::rng(Default::default());

            let queries = experiments::gen_many_to_many_queries(&graph, num_queries, 2usize.pow(ball_size_exp), 2usize.pow(target_set_size_exp), &mut rng);

            let mut algos_ctxt = push_collection_context("algo_runs");
            let mut many_to_one = chpot_data.potentials().0;
            many_to_one.init(*queries.last().unwrap().1.last().unwrap());
            many_to_one.potential(*queries.last().unwrap().0.last().unwrap());

            let mut rphast = RPHAST::new(chpot_data.backward_graph(), chpot_data.forward_graph(), chpot_data.order().clone());
            let mut rphast_query = RPHASTQuery::new(&rphast);

            let mut total_query_time = Duration::ZERO;

            for (sources, targets) in &queries {
                rphast.select(&sources);
                for &target in targets.choose_multiple(&mut rng, num_queries) {
                    let rphast_result = rphast_query.query(target, &rphast);
                    let _alg_ctx = algos_ctxt.push_collection_item();
                    report!("algo", "lazy_rphast_many_to_one");
                    let (_, time) = measure(|| {
                        silent_report_time_with_key("selection_running_time_ms", || {
                            many_to_one.init(target);
                        });
                        for &s in sources {
                            assert_eq!(many_to_one.potential(s).unwrap(), rphast_result.distance(s));
                        }
                    });
                    report!("running_time_ms", time.as_secs_f64() * 1000.0);
                    total_query_time = total_query_time + time;
                }
            }

            if num_queries > 0 {
                eprintln!(
                    "Avg. query time {}ms",
                    (total_query_time / (num_queries * num_queries) as u32).as_secs_f64() * 1000.0
                )
            };
        }
    }

    Ok(())
}
