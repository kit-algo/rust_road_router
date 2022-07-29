#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        customizable_contraction_hierarchy::*,
        traffic_aware::{td_traffic_pots::*, time_dependent::*},
        *,
    },
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        node_order::*,
    },
    experiments::chpot::num_queries,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("predicted_queries_smooth");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let queries = args.next().unwrap();
    let sources = Vec::<NodeId>::load_from(path.join(&queries).join("source"))?;
    let targets = Vec::<NodeId>::load_from(path.join(&queries).join("target"))?;
    let departures = Vec::<Timestamp>::load_from(path.join(&queries).join("departure"))?;
    let mut ranks = Vec::<u32>::load_from(path.join(&queries).join("rank")).ok().map(Vec::into_iter);
    let num_queries = std::cmp::min(num_queries(), sources.len());
    let query_iter = sources
        .into_iter()
        .zip(targets)
        .zip(departures)
        .map(|((from, to), at)| (from, to, at))
        .take(num_queries);
    let mut report_ranks = || {
        if let Some(ranks) = ranks.as_mut() {
            report!("rank", ranks.next().unwrap());
        }
    };

    let pot = args.next();

    let epsilon = args.next().map(|arg| arg.parse().expect("could not parse epsilon")).unwrap_or(1.0);
    report!("epsilon", epsilon);

    let graph = TDGraph::reconstruct_from(&path)?;
    let lower_bound: Vec<_> = (0..graph.num_arcs() as EdgeId).map(|e| graph.travel_time_function(e).lower_bound()).collect();
    let smooth_graph = BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound[..]);

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };

    let smooth_cch_pot = without_reporting(|| CCHPotData::new(&cch, &smooth_graph));

    match pot.as_deref() {
        Some("interval_min_pot") => {
            report!("potential", "interval_min_pot");
            let fl_graph = without_reporting(|| floating_time_dependent::TDGraph::reconstruct_from(&path))?;
            let catchup = without_reporting(|| customization::ftd_for_pot::customize::<96>(&cch, &fl_graph));
            let imp = without_reporting(|| IntervalMinPotential::new(&cch, catchup, smooth_graph.borrowed(), &graph));
            let mut server = TDTrafficAwareServer::new(smooth_graph.borrowed(), &graph, &smooth_cch_pot, imp);

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }, epsilon));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
        Some("multi_metric_pot") => {
            report!("potential", "multi_metric_pot");
            let mmp = without_reporting(|| MultiMetricPreprocessed::new(&cch, rust_road_router::algo::td_astar::ranges(), &graph, &smooth_graph, None));
            let mm_pot = MultiMetric::new(mmp, &graph);
            let mut server = TDTrafficAwareServer::new(smooth_graph.borrowed(), &graph, &smooth_cch_pot, mm_pot);

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }, epsilon));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
        Some("zero") => {
            report!("potential", "zero");
            let mut server = TDTrafficAwareServer::new(
                smooth_graph.borrowed(),
                &graph,
                &smooth_cch_pot,
                rust_road_router::algo::a_star::ZeroPotential(),
            );

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }, epsilon));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
        _ => {
            report!("potential", "lower_bound_cch_pot");
            let lower_bound = (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
                .collect::<Box<[Weight]>>();
            let cch_pot_data = without_reporting(|| CCHPotData::new(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound)));
            let mut server = TDTrafficAwareServer::new(smooth_graph.borrowed(), &graph, &smooth_cch_pot, cch_pot_data.forward_potential());

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }, epsilon));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
    }

    Ok(())
}
