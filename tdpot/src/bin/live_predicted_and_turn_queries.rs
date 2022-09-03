#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        a_star::TurnExpandedPotential,
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::{disconnected_targets::*, td_dijkstra::*},
        td_astar::*,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::{chpot::num_queries, run_td_queries},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("live_predicted_and_turn_queries");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("t_live", t_live);
    report!("live_data_file", live_data_file);

    let live_graph = (live_data_file, t_live).reconstruct_from(&path)?;
    let graph = live_graph.graph();

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(graph, order)
    };

    let forbidden_turn_from_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_from_arc"))?;
    let forbidden_turn_to_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_to_arc"))?;

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    let mut iter = forbidden_turn_from_arc.iter().zip(forbidden_turn_to_arc.iter()).peekable();

    let live_graph = live_graph.line_graph(|edge1_idx, edge2_idx| {
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
        if tail[edge1_idx as usize] == live_graph.graph().head()[edge2_idx as usize] {
            return None;
        }
        Some(0)
    });
    let n = live_graph.num_nodes();

    let mut rng = StdRng::from_seed(Default::default());
    let query_iter = std::iter::repeat_with(|| (rng.gen_range(0..n as NodeId), rng.gen_range(0..n as NodeId), t_live)).take(num_queries());

    let pot = args.next();

    match pot.as_deref() {
        Some("interval_min_pot") => {
            todo!("implement td turn exp pot");
            report!("potential", "interval_min_pot");
            let pot_in = args.next().unwrap_or("interval_min_pot".to_string());
            let interval_min_pot: IntervalMinPotential<LiveToPredictedBounds> = cch.reconstruct_from(&path.join(pot_in))?;
            let mut server = without_reporting(|| Server::new(&live_graph, interval_min_pot, PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
        }
        Some("multi_metric_pot") => {
            todo!("implement td turn exp pot");
            report!("potential", "multi_metric_pot");
            let pot_in = args.next().unwrap_or("multi_metric_pot".to_string());
            let mm_pot: MultiMetric = cch.reconstruct_from(&path.join(pot_in))?;
            let mut server = without_reporting(|| Server::new(&live_graph, mm_pot, PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
        }
        Some("zero") => {
            use rust_road_router::algo::a_star::ZeroPotential;
            report!("potential", "zero");
            let mut server = without_reporting(|| Server::new(&live_graph, ZeroPotential(), PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
        }
        Some("ch_potentials") => {
            report!("potential", "ch_pot");
            let potential = TurnExpandedPotential::new(graph, CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?);
            let mut server =
                without_reporting(|| CatchDisconnectedTarget::new(Server::new(&live_graph, potential, PessimisticLiveTDDijkstraOps::default()), &live_graph));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
        }
        _ => {
            report!("potential", "lower_bound_cch_pot");
            let lower_bound = (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
                .collect::<Box<[Weight]>>();
            let cch_pot_data = without_reporting(|| CCHPotData::new(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound)));
            let mut server = without_reporting(|| {
                CatchDisconnectedTarget::new(
                    Server::new(
                        &live_graph,
                        TurnExpandedPotential::new(graph, cch_pot_data.forward_potential()),
                        PessimisticLiveTDDijkstraOps::default(),
                    ),
                    &live_graph,
                )
            });

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
        }
    }

    Ok(())
}
