#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        customizable_contraction_hierarchy::{self, *},
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_nearest_neighbors");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let node_sampler: Box<[_]> = (0..n as NodeId).collect();
    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };
    let mut cch_nn = customizable_contraction_hierarchy::query::nearest_neighbor::SeparatorBasedNearestNeighbor::new(&cch, cch_pot_data.backward_potential());
    let mut lr_nn = customizable_contraction_hierarchy::query::nearest_neighbor::LazyRphastNearestNeighbor::new(cch_pot_data.backward_potential());
    let mut bcch_nn = customizable_contraction_hierarchy::query::nearest_neighbor::BCCHNearestNeighbor::new(cch_pot_data.customized());

    let num_targets_to_find = 4;
    let num_queries = 200;

    let mut exps_ctxt = push_collection_context("experiments");

    for target_set_size_exp in 1..=18 {
        // for target_set_size_exp in 14..=14 {
        let _exp_ctx = exps_ctxt.push_collection_item();

        report!("experiment", "nearest_neighbor");
        report!("target_set_size_exp", target_set_size_exp);
        report!("ball_size_exp", "full_graph");

        let mut rng = experiments::rng(Default::default());

        let mut queries = Vec::with_capacity(num_queries);
        for _ in 0..num_queries {
            let targets: Box<[_]> = node_sampler.choose_multiple(&mut rng, 2usize.pow(target_set_size_exp)).copied().collect();
            let source = rng.gen_range(0..n as NodeId);
            queries.push((source, targets));
        }

        let mut algos_ctxt = push_collection_context("algo_runs");
        let mut total_query_time = std::time::Duration::ZERO;
        let mut total_selection_time = std::time::Duration::ZERO;
        let mut total_time = std::time::Duration::ZERO;

        for (source, targets) in &queries {
            let _alg_ctx = algos_ctxt.push_collection_item();
            report_silent!("algo", "sep_based_cch_nearest_neighbor");
            let (_, time) = measure(|| {
                let (mut selection, selection_time) = measure(|| cch_nn.select_targets(&targets[..]));
                report_silent!("selection_time_ms", selection_time.as_secs_f64() * 1000.0);
                total_selection_time += selection_time;
                let (_, query_time) = measure(|| selection.query(*source, num_targets_to_find));
                report_silent!("query_time_ms", query_time.as_secs_f64() * 1000.0);
                total_query_time += query_time;
            });
            report_silent!("running_time_ms", time.as_secs_f64() * 1000.0);
            total_time += time;
        }

        if num_queries > 0 {
            eprintln!(
                "SepBased: Avg. selection time {}ms",
                (total_selection_time / num_queries as u32).as_secs_f64() * 1000.0
            );
            eprintln!("SepBased: Avg. query time {}ms", (total_query_time / num_queries as u32).as_secs_f64() * 1000.0);
            eprintln!(
                "SepBased: Avg. online query time {}ms",
                (total_time / num_queries as u32).as_secs_f64() * 1000.0
            );
        };

        let mut total_time = std::time::Duration::ZERO;

        for (source, targets) in &queries {
            let _alg_ctx = algos_ctxt.push_collection_item();
            report_silent!("algo", "lazy_rphast_nearest_neighbor");
            let (_, time) = measure(|| lr_nn.query(*source, &targets[..], num_targets_to_find));
            report_silent!("running_time_ms", time.as_secs_f64() * 1000.0);
            total_time += time;
        }

        if num_queries > 0 {
            eprintln!(
                "LazyRPHAST: Avg. online query time {}ms",
                (total_time / num_queries as u32).as_secs_f64() * 1000.0
            );
        };

        let mut total_query_time = std::time::Duration::ZERO;
        let mut total_selection_time = std::time::Duration::ZERO;
        let mut total_time = std::time::Duration::ZERO;

        for (source, targets) in &queries {
            let _alg_ctx = algos_ctxt.push_collection_item();
            report_silent!("algo", "sep_based_cch_nearest_neighbor");
            let (_, time) = measure(|| {
                let (mut selection, selection_time) = measure(|| bcch_nn.select_targets(&targets[..]));
                report_silent!("selection_time_ms", selection_time.as_secs_f64() * 1000.0);
                total_selection_time += selection_time;
                let (_, query_time) = measure(|| selection.query(*source, num_targets_to_find));
                report_silent!("query_time_ms", query_time.as_secs_f64() * 1000.0);
                total_query_time += query_time;
            });
            report_silent!("running_time_ms", time.as_secs_f64() * 1000.0);
            total_time += time;
        }

        if num_queries > 0 {
            eprintln!(
                "BCCH: Avg. selection time {}ms",
                (total_selection_time / num_queries as u32).as_secs_f64() * 1000.0
            );
            eprintln!("BCCH: Avg. query time {}ms", (total_query_time / num_queries as u32).as_secs_f64() * 1000.0);
            eprintln!("BCCH: Avg. online query time {}ms", (total_time / num_queries as u32).as_secs_f64() * 1000.0);
        };
    }

    Ok(())
}
