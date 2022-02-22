// WIP: CH potentials for TD Routing.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server, *},
        dijkstra::query::{dijkstra::Server as DijkServer, td_dijkstra::LiveTDDijkstraOps},
    },
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    experiments,
    io::*,
    report::*,
    util::in_range_option::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_td_live");

    let mut rng = experiments::rng(Default::default());

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let live_travel_time = Vec::<Weight>::load_from(path.join("live_travel_time"))?;

    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Vec<Weight>>();

    let mut live_count: usize = 0;
    let live = live_travel_time
        .iter()
        .zip(lower_bound.iter())
        .map(|(&live_tt, &lower_bound)| {
            if live_tt >= lower_bound {
                live_count += 1;
                Some(live_tt)
            } else {
                None
            }
        })
        .map(InRangeOption::new)
        .collect();

    report!("live_traffic", { "num_applied": live_count });

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    // let now = 15 * 3600 * 1000 + 41 * 60 * 1000;
    let now = 12 * 3600 * 1000;
    let soon = now + 3600 * 1000;

    let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;

    let graph = LiveTDGraph::new(graph, soon, live);

    affinity::set_thread_affinity(&[0]).unwrap();

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&graph, potential, LiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        now..now + 1,
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

    let mut server = DijkServer::<_, LiveTDDijkstraOps>::new(graph);

    experiments::run_random_td_queries(
        n,
        now..now + 1,
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
