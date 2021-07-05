#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{a_star::RecyclingPotential, ch_potentials::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_penalty");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let mut travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;

    let mut graph = FirstOutGraph::new(&first_out[..], &head[..], &mut travel_time[..]);
    unify_parallel_edges(&mut graph);
    drop(graph);
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs() });

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
    let forward_pot = RecyclingPotential::new(CHPotential::new(
        FirstOutGraph::new(&forward_first_out[..], &forward_head[..], &forward_weight[..]),
        FirstOutGraph::new(&backward_first_out[..], &backward_head[..], &backward_weight[..]),
        order.clone(),
    ));
    let backward_pot = RecyclingPotential::new(CHPotential::new(
        FirstOutGraph::new(&backward_first_out[..], &backward_head[..], &backward_weight[..]),
        FirstOutGraph::new(&forward_first_out[..], &forward_head[..], &forward_weight[..]),
        order.clone(),
    ));

    let mut penalty_server = {
        let _prepro_ctxt = algo_runs_ctxt.push_collection_item();
        penalty::Penalty::new(&graph, forward_pot)
    };

    let mut total_query_time = Duration::zero();
    let num_queries = experiments::chpot::num_queries();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);

        eprintln!();
        report!("from", from);
        report!("to", to);

        let (_, time) = measure(|| penalty_server.alternatives(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("pot_evals", penalty_server.potential().inner().num_pot_computations());
        eprintln!();

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
    };

    Ok(())
}
