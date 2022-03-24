#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::td_dijkstra::*,
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
    let _reporter = enable_reporting("live_queries");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let queries = args.next().unwrap();
    let sources = Vec::<NodeId>::load_from(path.join(&queries).join("source"))?;
    let targets = Vec::<NodeId>::load_from(path.join(&queries).join("target"))?;
    let mut ranks = Vec::<u32>::load_from(path.join(&queries).join("rank")).ok().map(Vec::into_iter);
    let query_iter = sources.into_iter().zip(targets).map(|(from, to)| (from, to, t_live)).take(num_queries());
    let mut report_ranks = || {
        if let Some(ranks) = ranks.as_mut() {
            report!("rank", ranks.next().unwrap());
        }
    };

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    let pot = args.next();

    report!("t_live", t_live);
    report!("live_data_file", live_data_file);

    let live_graph = (live_data_file, t_live).reconstruct_from(&path)?;
    let graph = live_graph.graph();

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(graph, order)
    };

    match pot.as_deref() {
        Some("interval_min_pot") => {
            report!("potential", "interval_min_pot");
            let pot_in = args.next().unwrap_or("interval_min_pot".to_string());
            let interval_min_pot: IntervalMinPotential<LiveToPredictedBounds> = cch.reconstruct_from(&path.join(pot_in))?;
            let mut server = without_reporting(|| Server::new(&live_graph, interval_min_pot, PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| report_ranks(), |_, _, _| None);
        }
        Some("multi_metric_pot") => {
            report!("potential", "multi_metric_pot");
            let pot_in = args.next().unwrap_or("multi_metric_pot".to_string());
            let mm_pot: MultiMetric = cch.reconstruct_from(&path.join(pot_in))?;
            let mut server = without_reporting(|| Server::new(&live_graph, mm_pot, PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| report_ranks(), |_, _, _| None);
        }
        Some("zero") => {
            use rust_road_router::algo::a_star::ZeroPotential;
            report!("potential", "zero");
            let mut server = without_reporting(|| Server::new(&live_graph, ZeroPotential(), PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| report_ranks(), |_, _, _| None);
        }
        _ => {
            report!("potential", "lower_bound_cch_pot");
            let lower_bound = (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
                .collect::<Box<[Weight]>>();
            let cch_pot_data = without_reporting(|| CCHPotData::new(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound)));
            let mut server = without_reporting(|| Server::new(&live_graph, cch_pot_data.forward_potential(), PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| report_ranks(), |_, _, _| None);
        }
    }

    Ok(())
}
