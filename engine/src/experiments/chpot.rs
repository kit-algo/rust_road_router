#[cfg(feature = "chpot-alt")]
use crate::algo::alt::ALTPotential;
#[cfg(any(feature = "chpot-cch", debug_assertions))]
use crate::algo::customizable_contraction_hierarchy::*;
use crate::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{generic_dijkstra::DefaultOps, query::dijkstra::Server as DijkServer},
        *,
    },
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};
use std::{error::Error, path::Path};

use rand::prelude::*;

/// Number of queries performed for each experiment.
/// Can be overriden through the CHPOT_NUM_QUERIES env var.
pub fn num_queries() -> usize {
    std::env::var("CHPOT_NUM_QUERIES").map_or(10000, |num| num.parse().unwrap())
}

pub fn run(
    path: &Path,
    modify_travel_time: impl FnOnce(&FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, &mut StdRng, &mut [Weight]) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut rng = super::rng(Default::default());

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

    #[cfg(any(feature = "chpot-cch", debug_assertions))]
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

    #[cfg(debug_assertions)]
    let mut cch_server = {
        let _blocked = block_reporting();
        customizable_contraction_hierarchy::query::Server::new(customize(&cch, &modified_graph))
    };

    let potential = {
        #[cfg(feature = "chpot-only-topo")]
        {
            ZeroPotential()
        }
        #[cfg(not(feature = "chpot-only-topo"))]
        {
            #[cfg(feature = "chpot-cch")]
            {
                let _potential_ctxt = algo_runs_ctxt.push_collection_item();
                CCHPotential::new(&cch, &graph)
            }
            #[cfg(feature = "chpot-alt")]
            {
                let _potential_ctxt = algo_runs_ctxt.push_collection_item();
                ALTPotential::new_with_avoid(&graph, 16, &mut rng)
            }
            #[cfg(all(not(feature = "chpot-cch"), not(feature = "chpot-alt")))]
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
        }
    };
    let potential = {
        #[cfg(feature = "chpot-oracle")]
        {
            RecyclingPotential::new(potential)
        }
        #[cfg(not(feature = "chpot-oracle"))]
        {
            potential
        }
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let infinity_filtered_graph = InfinityFilteringGraph(modified_graph);
    let mut topocore: TopoServer<OwnedGraph, _, _> = TopoServer::new(&infinity_filtered_graph, potential, DefaultOps::default());
    let InfinityFilteringGraph(modified_graph) = infinity_filtered_graph;
    drop(virtual_topocore_ctxt);

    super::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        num_queries(),
        |_from, _to, _server| {
            #[cfg(feature = "chpot-oracle")]
            {
                _server.query(Query { from: _from, to: _to });
            }
        },
        |mut res| {
            #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
            report!(
                "num_pot_computations",
                res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
            );
            report!(
                "lower_bound",
                res.as_mut()
                    .map(|res| {
                        let from = res.data().query().from();
                        res.data().lower_bound(from)
                    })
                    .flatten()
            );
        },
        |from, to| {
            #[cfg(debug_assertions)]
            {
                Some(cch_server.query(Query { from, to }).map(|res| res.distance()))
            }
            #[cfg(not(debug_assertions))]
            {
                None
            }
        },
    );

    let mut server = DijkServer::<_, DefaultOps>::new(modified_graph);
    super::run_random_queries(graph.num_nodes(), &mut server, &mut rng, &mut algo_runs_ctxt, super::num_dijkstra_queries());

    Ok(())
}
