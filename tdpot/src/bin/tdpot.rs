// WIP: CH potentials for TD Routing.

use rust_road_router::{
    algo::{
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::td_dijkstra::TDDijkstraOps,
        td_astar::*,
        *,
    },
    cli::CliErr,
    datastr::graph::time_dependent::*,
    datastr::graph::*,
    datastr::node_order::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdpot");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Box<[Weight]>>();

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound))
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&graph, cch_pot_data.forward_potential(), TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut server,
        &mut rng.clone(),
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        |_, _, _| None,
    );

    let multi_metric_pot = {
        let _blocked = block_reporting();
        MultiMetric::build(&cch, td_astar::ranges(), &graph)
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut mm_server = Server::new(&graph, multi_metric_pot, TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut mm_server,
        &mut rng.clone(),
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        |from, to, departure| {
            let _blocked = block_reporting();
            Some(server.td_query(TDQuery { from, to, departure }).distance())
        },
    );

    let customized_folder = path.join("customized_corridor_mins");
    let catchup = customization::ftd_for_pot::PotData::reconstruct_from(&customized_folder)?;
    let interval_min_pot = IntervalMinPotential::new(&cch, catchup);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut cb_server = Server::new(&graph, interval_min_pot, TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut cb_server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        |from, to, departure| {
            let _blocked = block_reporting();
            Some(server.td_query(TDQuery { from, to, departure }).distance())
        },
    );

    Ok(())
}
