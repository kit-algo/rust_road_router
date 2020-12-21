#[macro_use]
extern crate rust_road_router;
#[cfg(feature = "chpot-cch")]
use rust_road_router::algo::customizable_contraction_hierarchy::*;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{generic_dijkstra::DefaultOps, query::dijkstra::Server as DijkServer},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

use rand::prelude::*;
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    report!("program", "chpot_ranks");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::<Weight>::load_from(path.join("travel_time"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let increased_weights = travel_time.iter().map(|&w| w * 15 / 10).collect::<Vec<_>>();
    let modified_graph = FirstOutGraph::new(&first_out[..], &head[..], &increased_weights[..]);

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
    let mut topocore: TopoServer<_, _, OwnedGraph> = {
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

    for _i in 0..rust_road_router::experiments::NUM_DIJKSTRA_QUERIES {
        let from: NodeId = rng.gen_range(0, n as NodeId);

        server.ranks(from, |to, _dist, rank| {
            let _query_ctxt = algo_runs_ctxt.push_collection_item();
            let (mut res, time) = measure(|| QueryServer::query(&mut topocore, Query { from, to }));

            report!("from", from);
            report!("to", to);
            report!("rank", rank);
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            let dist = res.as_ref().map(|res| res.distance());
            report!("result", dist);
            res.as_mut().map(|res| res.path());
            report!("lower_bound", res.as_mut().map(|res| res.data().lower_bound(from)).flatten());
        });
    }

    Ok(())
}
