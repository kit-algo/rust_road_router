#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{query::dijkstra::Server as DijkServer, DefaultOps},
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

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore: TopoServer<OwnedGraph, _, _, true, true, true> = TopoServer::new(&modified_graph, potential, DefaultOps::default());
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
            report!("running_time_ms", time.as_secs_f64() * 1000.0);
            let dist = res.distance();
            report!("result", dist);
            res.node_path().expect("Dijkstra Rank Query should always have a path");
            report!("num_pot_computations", res.data().potential().num_pot_computations());
            report!("lower_bound", res.data().lower_bound(from));
        });
    }

    Ok(())
}
