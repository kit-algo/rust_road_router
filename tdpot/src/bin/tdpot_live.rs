// WIP: CH potentials for TD Routing.

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::td_dijkstra::*,
        td_astar::*,
        *,
    },
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        node_order::*,
    },
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdpot_live");

    let mut rng = experiments::rng(Default::default());

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let m = graph.num_arcs();
    let lower_bound = (0..m as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Box<[Weight]>>();

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    report!("num_cch_edges", cch.num_arcs());
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound))
    };

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    report!("t_live", t_live);
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("live_data_file", live_data_file);
    let live_graph = (graph, live_data_file, t_live).reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&live_graph, cch_pot_data.forward_potential(), PessimisticLiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);
    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut only_td_server = Server::new(live_graph.graph(), cch_pot_data.forward_potential(), TDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        t_live..=t_live,
        &mut server,
        &mut rng.clone(),
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        |_, _, _| None,
    );

    let multi_metric_pot = {
        let _blocked = block_reporting();
        let mut mmp = MultiMetricPreprocessed::new(&cch, td_astar::ranges(), live_graph.graph(), None);
        mmp.customize_live(&live_graph, t_live);
        MultiMetric::new(mmp)
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut mm_server = Server::new(&live_graph, multi_metric_pot, PessimisticLiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        t_live..=t_live,
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

    let mut total_live: u64 = 0;
    let mut total_td: u64 = 0;
    let mut num_affected: u64 = 0;

    let customized_folder = path.join("customized_corridor_mins");
    let catchup = customization::ftd_for_pot::PotData::reconstruct_from(&customized_folder)?;
    let interval_min_pot = report_time_with_key("pot_creation", "pot_creation", || {
        let _blocked = block_reporting();
        IntervalMinPotential::new_for_live(&cch, catchup, &live_graph, t_live)
    });

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut cb_server = Server::new(&live_graph, interval_min_pot, PessimisticLiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_td_queries(
        n,
        t_live..=t_live,
        &mut cb_server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        // |_, _, _| None,
        // |from, to, departure| {
        //     let _blocked = block_reporting();
        //     Some(server.td_query(TDQuery { from, to, departure }).distance())
        // },
        |from, to, departure| {
            let _blocked = block_reporting();
            let live_result = server.td_query(TDQuery { from, to, departure }).distance();
            let plain_td_result = only_td_server.td_query(TDQuery { from, to, departure }).distance();
            if let (Some(live_result), Some(plain_td_result)) = (live_result, plain_td_result) {
                total_live += live_result as u64;
                total_td += plain_td_result as u64;
                assert!(live_result >= plain_td_result);
                if live_result != plain_td_result {
                    num_affected += 1;
                }
            }
            Some(live_result)
        },
    );

    drop(algo_runs_ctxt);

    report!("num_queries_affected_by_live", num_affected);
    report!("avg_td_travel_time_s", total_td / 1000 / experiments::chpot::num_queries() as u64);
    report!("avg_td_live_travel_time_s", total_live / 1000 / experiments::chpot::num_queries() as u64);

    Ok(())
}
