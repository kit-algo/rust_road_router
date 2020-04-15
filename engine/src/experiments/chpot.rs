use crate::{
    algo::{
        ch_potentials::query::Server as TopoServer,
        customizable_contraction_hierarchy::{query::Server, *},
        *,
    },
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};
use std::{error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

pub fn run(
    path: &Path,
    modify_travel_time: impl FnOnce(&FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, &mut [Weight]) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let seed = Default::default();
    report!("seed", seed);

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

    modify_travel_time(&graph, &mut modified_travel_time)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

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
    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = contract(&graph, cch_order);
    drop(cch_build_ctxt);

    let mut modified_graph = FirstOutGraph::new(&first_out[..], &head[..], &mut modified_travel_time[..]);
    unify_parallel_edges(&mut modified_graph);
    drop(modified_graph);
    let modified_graph = FirstOutGraph::new(&first_out[..], &head[..], &modified_travel_time[..]);

    let cch_custom_ctxt = algo_runs_ctxt.push_collection_item();
    let mut cch_static_server = Server::new(customize(&cch, &graph));
    drop(cch_custom_ctxt);
    let cch_custom_ctxt = algo_runs_ctxt.push_collection_item();
    let mut cch_live_server = Server::new(customize(&cch, &modified_graph));
    drop(cch_custom_ctxt);

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(modified_graph.clone(), &cch, &graph, &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(modified_graph.clone(), &cch, &graph)
        }
    };
    drop(virtual_topocore_ctxt);

    let mut query_count = 0;
    let mut live_count = 0;
    let mut rng = StdRng::from_seed(seed);
    let mut total_query_time = Duration::zero();
    let mut live_query_time = Duration::zero();

    for _i in 0..100 {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let ground_truth = cch_live_server.query(Query { from, to }).map(|res| res.distance());

        report!("from", from);
        report!("to", to);
        report!("ground_truth", ground_truth.unwrap_or(INFINITY));

        query_count += 1;

        let lower_bound = cch_static_server.query(Query { from, to }).map(|res| res.distance());
        report!("lower_bound", lower_bound.unwrap_or(INFINITY));
        let (mut res, time) = measure(|| topocore.query(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist.unwrap_or(INFINITY));
        res.as_mut().map(|res| res.path());
        let live = lower_bound != ground_truth;
        if live {
            live_query_time = live_query_time + time;
            live_count += 1;
        }
        if dist != ground_truth {
            eprintln!("topo {:?} ground_truth {:?} ({} - {})", dist, ground_truth, from, to);
            assert!(ground_truth < dist);
        }

        total_query_time = total_query_time + time;
    }

    if query_count > 0 {
        eprintln!("Avg. query time {}", total_query_time / (query_count as i32))
    };
    if live_count > 0 {
        eprintln!("Avg. live query time {}", live_query_time / (live_count as i32))
    };

    Ok(())
}
