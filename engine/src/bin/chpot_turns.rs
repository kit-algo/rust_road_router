#[macro_use]
extern crate rust_road_router;
#[cfg(feature = "chpot-cch")]
use rust_road_router::{algo::customizable_contraction_hierarchy::*, datastr::node_order::NodeOrder};
use rust_road_router::{
    algo::{
        a_star::*,
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

use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_turns");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let forbidden_turn_from_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_from_arc"))?;
    let forbidden_turn_to_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_to_arc"))?;

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
        if tail[edge1_idx as usize] == graph.head()[edge2_idx as usize] {
            return None;
        }
        Some(0)
    });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    #[cfg(feature = "chpot-cch")]
    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };

    let potential = TurnExpandedPotential::new(&graph, {
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
    });

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore: TopoServer<OwnedGraph, _, _> = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(&exp_graph, potential, DefaultOps::default(), &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(&exp_graph, potential, DefaultOps::default())
        }
    };
    drop(virtual_topocore_ctxt);

    experiments::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _| (),
        |mut res| {
            #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
            report!(
                "num_pot_computations",
                res.as_mut().map(|res| res.data().potential().inner().num_pot_computations()).unwrap_or(0)
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
        |_, _| None,
    );

    let mut server = DijkServer::<_, DefaultOps>::new(exp_graph);

    experiments::run_random_queries(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut &mut algo_runs_ctxt,
        rust_road_router::experiments::num_dijkstra_queries(),
    );

    Ok(())
}
