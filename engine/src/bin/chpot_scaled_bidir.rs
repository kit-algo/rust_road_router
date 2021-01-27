#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        dijkstra::{generic_dijkstra::DefaultOps, query::bidirectional_dijkstra::Server},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments::a_star::NUM_QUERIES,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "bidir_chpot_scaling");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
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

        rust_road_router::experiments::chpot::run(path, |_graph, _rng, travel_time| {
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
    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let mut travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;
    let mut modified_travel_time = travel_time.clone();

    let mut graph = FirstOutGraph::new(&first_out[..], &head[..], &mut travel_time[..]);
    unify_parallel_edges(&mut graph);
    drop(graph);
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs() });

    modify_travel_time(&graph, &mut rng, &mut modified_travel_time)?;
    let mut modified_graph = FirstOutGraph::new(&first_out[..], &head[..], &mut modified_travel_time[..]);
    unify_parallel_edges(&mut modified_graph);
    drop(modified_graph);
    let modified_graph = FirstOutGraph::new(&first_out[..], &head[..], &modified_travel_time[..]);

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

    let mut total_query_time = Duration::zero();

    for _i in 0..NUM_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);

        report!("from", from);
        report!("to", to);

        let (mut res, time) = measure(|| QueryServer::query(&mut bidir_a_star, Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist);
        res.as_mut().map(|res| res.path());

        total_query_time = total_query_time + time;
    }

    if NUM_QUERIES > 0 {
        eprintln!("Avg. query time {}", total_query_time / (NUM_QUERIES as i32))
    };

    Ok(())
}
