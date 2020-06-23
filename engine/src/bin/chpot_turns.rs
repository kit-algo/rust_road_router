#[macro_use]
extern crate bmw_routing_engine;
#[cfg(feature = "chpot-cch")]
use bmw_routing_engine::algo::customizable_contraction_hierarchy::*;
use bmw_routing_engine::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::query::dijkstra::Server as DijkServer,
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

use rand::prelude::*;
use std::{env, error::Error, path::Path};
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    report!("program", "chpot_turns");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;

    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let forbidden_turn_from_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_from_arc"))?;
    let forbidden_turn_to_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_to_arc"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    let mut iter = forbidden_turn_from_arc.iter().zip(forbidden_turn_to_arc.iter()).peekable();

    let exp_graph = graph.line_graph(|edge1_idx, edge2_idx| {
        while let Some((&from_arc, &to_arc)) = iter.peek() {
            if from_arc < edge1_idx || (from_arc == edge1_idx && to_arc < edge2_idx) {
                iter.next();
            } else {
                break;
            }
        }

        if iter.peek() == Some(&(&edge1_idx, &edge2_idx)) {
            return None;
        }
        if tail[edge1_idx as usize] == head[edge2_idx as usize] {
            return None;
        }
        Some(0)
    });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    #[cfg(feature = "chpot-cch")]
    let cch = {
        let cch_order = Vec::load_from(path.join("cch_perm"))?;
        let cch_order = NodeOrder::from_node_order(cch_order);

        let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
        let cch = contract(&graph, cch_order);
        drop(cch_build_ctxt);
        let cch_order = CCHReordering {
            cch: &cch,
            latitude: &[],
            longitude: &[],
        }
        .reorder_for_seperator_based_customization();
        let _cch_build_ctxt = algo_runs_ctxt.push_collection_item();
        contract(&graph, cch_order)
    };

    let potential = TurnExpandedPotential::new(&graph, {
        #[cfg(feature = "chpot-cch")]
        {
            let _potential_ctxt = algo_runs_ctxt.push_collection_item();
            CCHPotential::new(&cch, &graph)
        }
        #[cfg(not(feature = "chpot-cch"))]
        {
            let forward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/forward_first_out"))?;
            let forward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/forward_head"))?;
            let forward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/forward_weight"))?;
            let backward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/backward_first_out"))?;
            let backward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/backward_head"))?;
            let backward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/backward_weight"))?;
            let order = NodeOrder::from_node_order(Vec::<NodeId>::load_from(path.join("lower_bound_ch/order"))?);
            CHPotential::new(
                OwnedGraph::new(forward_first_out, forward_head, forward_weight),
                OwnedGraph::new(backward_first_out, backward_head, backward_weight),
                order,
            )
        }
    });

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(exp_graph.clone(), potential, &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(exp_graph.clone(), potential)
        }
    };
    drop(virtual_topocore_ctxt);

    let mut query_count = 0;
    let mut total_query_time = Duration::zero();

    let n = exp_graph.num_nodes();
    for _i in 0..bmw_routing_engine::experiments::chpot::NUM_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, n as NodeId);
        let to: NodeId = rng.gen_range(0, n as NodeId);

        let (mut res, time) = measure(|| topocore.query(Query { from, to }));

        query_count += 1;

        report!("from", from);
        report!("to", to);
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist);
        res.as_mut().map(|res| res.path());
        report!("lower_bound", res.as_mut().map(|res| res.data().lower_bound(from)).flatten());

        total_query_time = total_query_time + time;
    }

    if query_count > 0 {
        eprintln!("Avg. query time {}", total_query_time / (query_count as i32))
    };

    let mut server = DijkServer::new(exp_graph);

    for _i in 0..bmw_routing_engine::experiments::NUM_DIJKSTRA_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, n as NodeId);
        let to: NodeId = rng.gen_range(0, n as NodeId);

        report!("from", from);
        report!("to", to);

        query_count += 1;

        let (res, time) = measure(|| server.query(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist);
    }

    Ok(())
}
