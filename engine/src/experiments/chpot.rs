#[cfg(feature = "chpot-alt")]
use crate::algo::alt::ALTPotential;
#[cfg(any(feature = "chpot-cch", debug_assertions))]
use crate::{algo::customizable_contraction_hierarchy::*, datastr::node_order::NodeOrder};
use crate::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{generic_dijkstra::DefaultOps, query::dijkstra::Server as DijkServer},
        *,
    },
    datastr::graph::*,
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

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut modified_travel_time = graph.weight().to_vec();

    modify_travel_time(&graph.borrowed(), &mut rng, &mut modified_travel_time)?;
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    #[cfg(any(feature = "chpot-cch", debug_assertions))]
    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
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
                CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?
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
        // |mut res| {
        //     #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
        //     report!(
        //         "num_pot_computations",
        //         res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
        //     );
        //     report!(
        //         "lower_bound",
        //         res.as_mut()
        //             .map(|res| {
        //                 let from = res.data().query().from();
        //                 res.data().lower_bound(from)
        //             })
        //             .flatten()
        //     );
        // },
        |_from, _to| {
            #[cfg(debug_assertions)]
            {
                Some(cch_server.query(Query { from: _from, to: _to }).map(|res| res.distance()))
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
