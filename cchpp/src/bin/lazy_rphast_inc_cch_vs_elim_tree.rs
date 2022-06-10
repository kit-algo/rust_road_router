#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{a_star::*, ch_potentials::*, customizable_contraction_hierarchy::*},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("lazy_rphast");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let num_queries = 100;
    let target_set_size_exp = 14;

    let mut exps_ctxt = push_collection_context("experiments");

    for ball_size_exp in 14..=24 {
        let _exp_ctx = exps_ctxt.push_collection_item();

        report!("experiment", "lazy_rphast");
        report!("target_set_size_exp", target_set_size_exp);
        report!("ball_size_exp", ball_size_exp);

        let mut rng = experiments::rng(Default::default());

        let queries = experiments::gen_many_to_many_queries(&graph, num_queries, 2usize.pow(ball_size_exp), 2usize.pow(target_set_size_exp), &mut rng);

        let mut algos_ctxt = push_collection_context("algo_runs");
        let mut many_to_one = cch_pot_data.ch_forward_potential();
        many_to_one.init(*queries.last().unwrap().1.last().unwrap());
        many_to_one.potential(*queries.last().unwrap().0.last().unwrap());

        for (sources, targets) in &queries {
            for &target in targets.choose_multiple(&mut rng, num_queries) {
                let _alg_ctx = algos_ctxt.push_collection_item();
                report!("algo", "lazy_rphast_many_to_one");
                silent_report_time_with_key("selection_running_time_ms", || {
                    many_to_one.init(target);
                });
                let mut progress = push_collection_context("progress_states");
                let timer = Timer::new();
                for (i, &s) in sources.iter().enumerate() {
                    let i = i + 1;
                    many_to_one.potential(s);
                    if (i & (i - 1)) == 0 {
                        let _state_ctx = progress.push_collection_item();
                        report!("terminal_rank", i.trailing_zeros());
                        report!("passed_time_mus", timer.get_passed().as_micros() as u64);
                    }
                }
            }
        }

        let mut many_to_one = cch_pot_data.forward_potential();
        many_to_one.init(*queries.last().unwrap().1.last().unwrap());
        many_to_one.potential(*queries.last().unwrap().0.last().unwrap());

        for (sources, targets) in &queries {
            for &target in targets.choose_multiple(&mut rng, num_queries) {
                let _alg_ctx = algos_ctxt.push_collection_item();
                report!("algo", "lazy_rphast_cch_many_to_one");
                silent_report_time_with_key("selection_running_time_ms", || {
                    many_to_one.init(target);
                });
                let mut progress = push_collection_context("progress_states");
                let timer = Timer::new();
                for (i, &s) in sources.iter().enumerate() {
                    let i = i + 1;
                    many_to_one.potential(s);
                    if (i & (i - 1)) == 0 {
                        let _state_ctx = progress.push_collection_item();
                        report!("terminal_rank", i.trailing_zeros());
                        report!("passed_time_mus", timer.get_passed().as_micros() as u64);
                    }
                }
            }
        }
    }

    Ok(())
}
