#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::td_dijkstra::*,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::{chpot::num_queries, run_td_queries},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("live_and_predicted_queries");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("t_live", t_live);
    report!("live_data_file", live_data_file);

    let mut live_graph = (live_data_file, t_live).reconstruct_from(&path)?;
    live_graph.to_constant_lower();
    let graph = live_graph.graph();

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(graph, order)
    };

    let queries = args.next().unwrap();
    let sources = Vec::<NodeId>::load_from(path.join(&queries).join("source"))?;
    let targets = Vec::<NodeId>::load_from(path.join(&queries).join("target"))?;
    let mut ranks = Vec::<u32>::load_from(path.join(&queries).join("rank")).ok().map(Vec::into_iter);
    let mut report_ranks = || {
        if let Some(ranks) = ranks.as_mut() {
            report!("rank", ranks.next().unwrap());
        }
    };
    let query_iter = sources.into_iter().zip(targets).map(|(from, to)| (from, to, t_live)).take(num_queries());

    let pot = args.next();

    match pot.as_deref() {
        Some("zero") => {
            use rust_road_router::algo::a_star::ZeroPotential;
            report!("potential", "zero");
            let mut server = without_reporting(|| Server::new_no_topo(&live_graph, ZeroPotential(), PessimisticLiveTDDijkstraOps::default()));

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            run_td_queries(query_iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| report_ranks(), |_, _, _| None);
        }
        Some("ch_potentials") => {
            report!("potential", "ch_pot");
            let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;
            let mut server = without_reporting(|| Server::new(&live_graph, potential, PessimisticLiveTDDijkstraOps::default()));

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
