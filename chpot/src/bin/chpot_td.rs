// WIP: CH potentials for TD Routing.

use rust_road_router::{
    algo::{
        ch_potentials::{query::Server, *},
        dijkstra::query::{dijkstra::Server as DijkServer, td_dijkstra::TDDijkstraOps},
    },
    cli::CliErr,
    datastr::graph::time_dependent::*,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_td");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&graph, potential, TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        // |res| {
        //     report!("num_pot_computations", res.data().potential().num_pot_computations());
        //     report!("lower_bound", res.data().lower_bound(from));
        // },
        |_, _, _| None,
    );

    let mut server = DijkServer::<_, TDDijkstraOps, _>::new(graph);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::num_dijkstra_queries(),
        |_, _, _, _| (),
        // |_| (),
        |_, _, _| None,
    );

    Ok(())
}
