#[macro_use]
extern crate rust_road_router;
#[cfg(feature = "chpot-cch")]
use rust_road_router::{algo::customizable_contraction_hierarchy::*, datastr::node_order::NodeOrder};
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{generic_dijkstra::DefaultOps, query::dijkstra::Server as DijkServer},
        *,
    },
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};

use rand::prelude::*;
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_ranks");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    let increased_weights = graph.weight().iter().map(|&w| w * 15 / 10).collect::<Vec<_>>();
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &increased_weights[..]);

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    #[cfg(feature = "chpot-cch")]
    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
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
            #[cfg(not(feature = "chpot-cch"))]
            {
                CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?
            }
        }
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore: TopoServer<OwnedGraph, _, _> = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(&modified_graph, potential, DefaultOps::default(), &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(&modified_graph, potential, DefaultOps::default())
        }
    };
    drop(virtual_topocore_ctxt);

    let n = graph.num_nodes();

    let mut server = DijkServer::<_, DefaultOps, _>::new(graph);

    for _i in 0..experiments::num_dijkstra_queries() {
        let from: NodeId = rng.gen_range(0..n as NodeId);

        server.ranks(from, |to, _dist, rank| {
            let _query_ctxt = algo_runs_ctxt.push_collection_item();
            let (mut res, time) = measure(|| topocore.query(Query { from, to }));

            report!("from", from);
            report!("to", to);
            report!("rank", rank);
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            let dist = res.as_ref().map(|res| res.distance());
            report!("result", dist);
            res.as_mut().map(|res| res.path());
            #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
            report!(
                "num_pot_computations",
                res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
            );
            report!("lower_bound", res.as_mut().map(|res| res.data().lower_bound(from)).flatten());
        });
    }

    Ok(())
}
