#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        customizable_contraction_hierarchy::*,
        traffic_aware::{td_live::*, td_traffic_pots::*},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::chpot::num_queries,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("live_and_predicted_queries_smooth");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("t_live", t_live);
    report!("live_data_file", live_data_file);

    let live_graph = (live_data_file, t_live).reconstruct_from(&path)?;
    let graph = live_graph.graph();

    let queries = args.next().unwrap();
    report!("queries", queries);
    let sources = Vec::<NodeId>::load_from(path.join(&queries).join("source"))?;
    let targets = Vec::<NodeId>::load_from(path.join(&queries).join("target"))?;
    let mut ranks = Vec::<u32>::load_from(path.join(&queries).join("rank")).ok().map(Vec::into_iter);
    let num_queries = std::cmp::min(num_queries(), sources.len());
    let query_iter = sources
        .into_iter()
        .zip(targets)
        .zip(std::iter::repeat(t_live))
        .map(|((from, to), at)| (from, to, at))
        .take(num_queries);
    let mut report_ranks = || {
        if let Some(ranks) = ranks.as_mut() {
            report!("rank", ranks.next().unwrap());
        }
    };

    let variant = args.next();

    let epsilon = args.next().map(|arg| arg.parse().expect("could not parse epsilon")).unwrap_or(1.0);
    report!("epsilon", epsilon);

    let lower_bound: Vec<_> = (0..graph.num_arcs() as EdgeId).map(|e| graph.travel_time_function(e).lower_bound()).collect();
    assert!(lower_bound.iter().all(|&l| l > 0));
    let smooth_graph = BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound[..]);

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(graph, order)
    };

    let smooth_cch_pot = without_reporting(|| CCHPotData::new(&cch, &smooth_graph));

    report!("potential", "interval_min_pot");
    let pot_in = args.next().unwrap_or("interval_min_pot".to_string());
    let imp: IntervalMinPotential<(Weight, Weight)> = cch.reconstruct_from(&path.join(pot_in))?;

    match variant.as_deref() {
        Some("iterative_path_blocking") => {
            let mut server = TDTrafficAwareServer::new(smooth_graph.borrowed(), &live_graph, &smooth_cch_pot, imp);

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
        Some("iterative_detour_blocking") => {
            let mut server = HeuristicTDTrafficAwareServer::new(smooth_graph.borrowed(), &live_graph, &smooth_cch_pot, imp);

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }, epsilon, |_| ()));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
        Some("iterative_path_fixing") => {
            let mut server = IterativePathFixing::new(smooth_graph.borrowed(), &live_graph, &smooth_cch_pot, imp);

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
        Some("smooth_path_baseline") => {
            let mut server = SmoothPathBaseline::new(smooth_graph.borrowed(), &live_graph, &smooth_cch_pot, imp);

            let mut algo_runs_ctxt = push_collection_context("algo_runs");
            let mut total_query_time = std::time::Duration::ZERO;

            for (from, to, departure) in query_iter.take(num_queries) {
                let _query_ctxt = algo_runs_ctxt.push_collection_item();

                report!("from", from);
                report!("to", to);
                report!("at", departure);
                report_ranks();

                let (_, time) = measure(|| server.query(TDQuery { from, to, departure }));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);

                total_query_time = total_query_time + time;
            }

            if num_queries > 0 {
                eprintln!("Avg. query time {}ms", (total_query_time / (num_queries as u32)).as_secs_f64() * 1000.0)
            };
        }
        _ => panic!("need variant"),
    }

    Ok(())
}
