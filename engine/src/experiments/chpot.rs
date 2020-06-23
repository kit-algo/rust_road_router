#[cfg(feature = "chpot-cch")]
use crate::algo::customizable_contraction_hierarchy::*;
use crate::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::query::dijkstra::Server as DijkServer,
        *,
    },
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};
use std::{error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

/// Number of queries performed for each experiment.
/// Can be overriden through the CHPOT_NUM_QUERIES env var.
#[cfg(not(override_chpot_num_queries))]
pub const NUM_QUERIES: usize = 10000;
#[cfg(override_chpot_num_queries)]
pub const NUM_QUERIES: usize = include!(concat!(env!("OUT_DIR"), "/CHPOT_NUM_QUERIES"));

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
    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;
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

    let potential = {
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
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(modified_graph.clone(), potential, &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(modified_graph.clone(), potential)
        }
    };
    drop(virtual_topocore_ctxt);

    let mut query_count = 0;
    let mut total_query_time = Duration::zero();

    for _i in 0..NUM_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);

        report!("from", from);
        report!("to", to);

        query_count += 1;

        let (mut res, time) = measure(|| topocore.query(Query { from, to }));
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

    let mut server = DijkServer::new(modified_graph);

    for _i in 0..super::NUM_DIJKSTRA_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);

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
