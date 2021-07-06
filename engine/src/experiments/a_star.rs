#[cfg(feature = "chpot-alt")]
use crate::algo::alt::ALTPotential;
#[cfg(feature = "chpot-cch")]
use crate::algo::customizable_contraction_hierarchy::*;
use crate::{
    algo::{
        ch_potentials::*,
        dijkstra::{generic_dijkstra::DefaultOps, query::dijkstra::Server as DijkServer},
    },
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};
use std::{error::Error, path::Path};

use rand::prelude::*;

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

    let mut topocore = DijkServer::<_, DefaultOps, _>::with_potential(modified_graph, potential);

    experiments::run_random_queries_with_pre_callback(
        graph.num_nodes(),
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        super::chpot::num_queries(),
        |_from, _to, _server| {
            #[cfg(feature = "chpot-oracle")]
            {
                _server.query(Query { from: _from, to: _to });
            }
        },
    );

    Ok(())
}
