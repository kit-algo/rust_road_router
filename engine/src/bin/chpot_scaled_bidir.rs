#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{ch_potentials::*, dijkstra::query::bidirectional_dijkstra::Server},
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("bidir_chpot_scaling");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    for factor in [
        1., 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.1, 1.11, 1.12, 1.13, 1.14, 1.15, 1.16, 1.17, 1.18, 1.19, 1.2, 1.21, 1.22, 1.23, 1.24, 1.25,
    ]
    .iter()
    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        run(path, |_graph, _rng, travel_time| {
            for weight in travel_time.iter_mut() {
                *weight = (*weight as f64 * factor) as Weight;
            }

            Ok(())
        })?;
    }

    Ok(())
}

pub fn run(
    path: &Path,
    modify_travel_time: impl FnOnce(&FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, &mut StdRng, &mut [Weight]) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut rng = experiments::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut modified_travel_time = graph.weight().to_vec();

    modify_travel_time(&graph.borrowed(), &mut rng, &mut modified_travel_time)?;
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let forward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/forward_first_out"))?;
    let forward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/forward_head"))?;
    let forward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/forward_weight"))?;
    let backward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/backward_first_out"))?;
    let backward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/backward_head"))?;
    let backward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/backward_weight"))?;
    let order = NodeOrder::from_node_order(Vec::<NodeId>::load_from(path.join("lower_bound_ch/order"))?);
    let forward_pot = CHPotential::new(
        FirstOutGraph::new(&forward_first_out[..], &forward_head[..], &forward_weight[..]),
        FirstOutGraph::new(&backward_first_out[..], &backward_head[..], &backward_weight[..]),
        order.clone(),
    );
    let backward_pot = CHPotential::new(
        FirstOutGraph::new(&backward_first_out[..], &backward_head[..], &backward_weight[..]),
        FirstOutGraph::new(&forward_first_out[..], &forward_head[..], &forward_weight[..]),
        order.clone(),
    );

    let mut bidir_a_star = Server::new_with_potentials(modified_graph, forward_pot, backward_pot);

    experiments::run_random_queries(
        graph.num_nodes(),
        &mut bidir_a_star,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
    );

    Ok(())
}
