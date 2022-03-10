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
    util::in_range_option::*,
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

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    report!("t_live", t_live);
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("live_data_file", live_data_file);
    let mut live = vec![InRangeOption::NONE; m];
    let live_data = Vec::<(EdgeId, Weight, Weight)>::load_from(path.join(live_data_file))?;
    report!("num_edges_with_live", live_data.len());
    let mut not_really_live: usize = 0;
    let mut blocked: usize = 0;
    let mut blocked_but_also_long_term: usize = 0;
    let max_t_soon = period();
    report!("max_t_soon", max_t_soon);
    let mut max_duration = 0;
    for (edge, weight, duration) in live_data {
        if weight >= INFINITY {
            blocked += 1;
        }
        if duration < max_t_soon {
            live[edge as usize] = InRangeOption::some((weight, duration + t_live));
            max_duration = std::cmp::max(max_duration, duration);
        } else {
            if weight >= INFINITY && duration >= max_t_soon {
                blocked_but_also_long_term += 1;
            }
            not_really_live += 1;
        }
    }
    report!("num_long_term_live_reports", not_really_live);
    report!("blocked", blocked);
    report!("num_long_term_blocks", blocked_but_also_long_term);

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

    let live_graph = PessimisticLiveTDGraph::new(graph, live);
    let upper_bound = (0..m as EdgeId).map(|edge_id| live_graph.upper_bound(edge_id)).collect::<Box<[Weight]>>();
    let upper_bound_customized = {
        let _blocked = block_reporting();
        let mut customized = customize(
            &cch,
            &FirstOutGraph::new(live_graph.graph().first_out(), live_graph.graph().head(), &upper_bound),
        );
        customization::customize_perfect_without_rebuild(&mut customized);
        customized
    };

    let customized_folder = path.join("customized_corridor_mins");
    let mut catchup = customization::ftd_for_pot::PotData::reconstruct_from(&customized_folder)?;
    let fw_non_live_upper = catchup.fw_static_bound.iter().map(|&(_l, u)| u).collect();
    let bw_non_live_upper = catchup.bw_static_bound.iter().map(|&(_l, u)| u).collect();
    let mut worse_uppers: u64 = 0;
    for ((_, upper), live) in catchup.fw_static_bound.iter_mut().zip(upper_bound_customized.forward_graph().weight()) {
        if *upper < *live {
            worse_uppers += 1;
        }
        *upper = std::cmp::max(*upper, *live);
    }
    for ((_, upper), live) in catchup.bw_static_bound.iter_mut().zip(upper_bound_customized.backward_graph().weight()) {
        if *upper < *live {
            worse_uppers += 1;
        }
        *upper = std::cmp::max(*upper, *live);
    }
    report!("num_worse_upper_bounds", worse_uppers);

    let interval_min_pot = IntervalMinPotential::new(&cch, catchup, fw_non_live_upper, bw_non_live_upper, max_duration + t_live);

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

    // let mut ranges = vec![0..24 * 60 * 60 * 1000];
    // for i in 0..48 {
    //     ranges.push(i * 30 * 60 * 1000..(i + 3) * 30 * 60 * 1000)
    // }

    // let multi_metric_pot = {
    //     let _blocked = block_reporting();
    //     MultiMetric::build(&cch, ranges, &graph)
    // };

    // let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    // let mut mm_server = Server::new(&graph, multi_metric_pot, TDDijkstraOps::default());
    // drop(virtual_topocore_ctxt);

    // experiments::run_random_td_queries(
    //     n,
    //     t_live..=t_live,
    //     &mut mm_server,
    //     &mut rng.clone(),
    //     &mut algo_runs_ctxt,
    //     experiments::chpot::num_queries(),
    //     |_, _, _, _| (),
    //     |from, to, departure| {
    //         let _blocked = block_reporting();
    //         Some(server.td_query(TDQuery { from, to, departure }).distance())
    //     },
    // );

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut cb_server = Server::new(&live_graph, interval_min_pot, PessimisticLiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    let mut total_live: u64 = 0;
    let mut total_td: u64 = 0;
    let mut num_affected: u64 = 0;

    experiments::run_random_td_queries(
        n,
        t_live..=t_live,
        &mut cb_server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _, _| (),
        // |_, _, _| None,
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
