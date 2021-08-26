// WIP: CH potentials for TD Routing.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        a_star::*,
        ch_potentials::{query::Server, *},
        dijkstra::query::{dijkstra::Server as DijkServer, disconnected_targets::*, td_dijkstra::LiveTDDijkstraOps},
    },
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    experiments,
    io::*,
    report::*,
    util::in_range_option::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_turns_td_live");

    let mut rng = experiments::rng(Default::default());

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let live_travel_time = Vec::<Weight>::load_from(path.join("live_travel_time"))?;

    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Vec<Weight>>();

    let mut live_count = 0;
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

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let now = 12 * 3600 * 1000;
    let soon = now + 3600 * 1000;

    let potential = TurnExpandedPotential::new(&graph, CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?);

    let graph = LiveTDGraph::new(graph, soon, live);

    let forbidden_turn_from_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_from_arc"))?;
    let forbidden_turn_to_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_to_arc"))?;

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    let mut iter = forbidden_turn_from_arc.iter().zip(forbidden_turn_to_arc.iter()).peekable();

    let graph = graph.line_graph(|edge1_idx, edge2_idx| {
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
        if tail[edge1_idx as usize] == graph.graph().head()[edge2_idx as usize] {
            return None;
        }
        Some(0)
    });
    let n = graph.num_nodes();

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let server = Server::new(&graph, potential, LiveTDDijkstraOps::default());
    let mut server = CatchDisconnectedTarget::new(server, &graph);
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        now..now + 1,
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _| (),
        // |res| {
        //     report!("num_pot_computations", res.data().potential().inner().num_pot_computations());
        //     report!("lower_bound", res.data().lower_bound(from));
        // },
        |_, _| None,
    );

    let mut server = DijkServer::<_, LiveTDDijkstraOps>::new(graph);

    experiments::run_random_td_queries(
        n,
        now..now + 1,
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::num_dijkstra_queries(),
        |_, _, _| (),
        // |_| (),
        |_, _| None,
    );

    Ok(())
}
