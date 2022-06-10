#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        customizable_contraction_hierarchy::{self, *},
        dijkstra,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_nearest_neighbors_from_entire_graph");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let n = graph.num_nodes();
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
    let mut dijkstra_nn = dijkstra::query::nearest_neighbor::DijkstraNearestNeighbor::new(graph.borrowed());

    let num_queries = 100;

    let mut exps_ctxt = push_collection_context("experiments");

    for target_set_size_exp in [10, 12, 14] {
        for ball_size_exp in 14..=24 {
            for num_targets_to_find in [1, 4, 8] {
                for sources_in_ball in [true, false] {
                    let _exp_ctx = exps_ctxt.push_collection_item();

                    report!("experiment", "nearest_neighbor");
                    report!("target_set_size_exp", target_set_size_exp);
                    report!("num_targets_to_find", num_targets_to_find);
                    report!("ball_size_exp", ball_size_exp);
                    report!("sources_in_ball", sources_in_ball);

                    let mut rng = experiments::rng(Default::default());

                    let mut queries =
                        experiments::gen_many_to_many_queries(&graph, num_queries, 2usize.pow(ball_size_exp), 2usize.pow(target_set_size_exp), &mut rng);
                    for (sources, _targets) in &mut queries {
                        if sources_in_ball {
                            sources.truncate(num_queries);
                        } else {
                            *sources = std::iter::repeat_with(|| rng.gen_range(0..n as NodeId)).take(num_queries).collect();
                        }
                    }

                    let mut algos_ctxt = push_collection_context("algo_runs");

                    for (sources, targets) in &queries {
                        let mut selection = {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "sep_based_cch_nearest_neighbor_selection");
                            silent_report_time(|| cch_nn.select_targets(&targets[..]))
                        };
                        for source in &sources[..] {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "sep_based_cch_nearest_neighbor_query");
                            silent_report_time(|| selection.query(*source, num_targets_to_find));
                        }
                    }

                    for (sources, targets) in &queries {
                        for source in &sources[..] {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "lazy_rphast_nearest_neighbor");
                            silent_report_time(|| lr_nn.query(*source, &targets[..], num_targets_to_find));
                        }
                    }

                    for (sources, targets) in &queries {
                        let mut selection = {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "bcch_nearest_neighbor_selection");
                            silent_report_time(|| bcch_nn.select_targets(&targets[..]))
                        };
                        for source in &sources[..] {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "bcch_nearest_neighbor_query");
                            silent_report_time(|| selection.query(*source, num_targets_to_find));
                        }
                    }

                    for (sources, targets) in &queries {
                        let mut selection = {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "dijkstra_nearest_neighbor_selection");
                            silent_report_time(|| dijkstra_nn.select_targets(&targets[..]))
                        };
                        for source in sources.iter().take(5) {
                            let _alg_ctx = algos_ctxt.push_collection_item();
                            report_silent!("algo", "dijkstra_nearest_neighbor_query");
                            silent_report_time(|| selection.query(*source, num_targets_to_find));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
