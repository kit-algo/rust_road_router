// WIP: CH potentials for TD Routing.

use std::{env, error::Error, path::Path};
#[macro_use]
extern crate rust_road_router;
#[cfg(feature = "chpot-cch")]
use rust_road_router::algo::customizable_contraction_hierarchy::*;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server, *},
        dijkstra::query::{dijkstra::Server as DijkServer, td_dijkstra::TDDijkstraOps},
        *,
    },
    cli::CliErr,
    datastr::graph::*,
    datastr::{graph::time_dependent::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};

use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_td");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();

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
                let lower_bound = (0..graph.num_arcs() as EdgeId)
                    .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
                    .collect::<Vec<Weight>>();

                let _potential_ctxt = algo_runs_ctxt.push_collection_item();
                CCHPotential::new(&cch, &FirstOutGraph::new(graph.first_out(), graph.head(), lower_bound))
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
        }
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&graph, potential, TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    let num_queries = experiments::chpot::num_queries();

    let mut astar_time = Duration::zero();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..n as NodeId);
        let to: NodeId = rng.gen_range(0..n as NodeId);
        let at: NodeId = rng.gen_range(0..period() as Timestamp);

        report!("from", from);
        report!("to", to);
        report!("at", at);
        let (mut res, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("result", res.as_ref().map(|res| res.distance()));
        #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
        report!(
            "num_pot_computations",
            res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
        );
        report!("lower_bound", res.as_mut().map(|res| res.data().lower_bound(from)).flatten());
        astar_time = astar_time + time;
    }
    eprintln!("A* {}", astar_time / (num_queries as i32));

    let num_queries = experiments::num_dijkstra_queries();

    let mut server = DijkServer::<_, TDDijkstraOps, _>::new(graph);

    let mut dijkstra_time = Duration::zero();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..n as NodeId);
        let to: NodeId = rng.gen_range(0..n as NodeId);
        let at: NodeId = rng.gen_range(0..period() as Timestamp);

        report!("from", from);
        report!("to", to);
        report!("at", at);
        let (ea, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }).map(|res| res.distance()));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("result", ea);
        dijkstra_time = dijkstra_time + time;
    }
    if num_queries > 0 {
        eprintln!("Dijk* {}", dijkstra_time / (num_queries as i32));
    }

    Ok(())
}
