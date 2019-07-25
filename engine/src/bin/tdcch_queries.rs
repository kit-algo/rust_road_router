use std::{
    path::Path,
    env,
};

#[macro_use] extern crate bmw_routing_engine;
use bmw_routing_engine::{
    graph::{
        *,
        floating_time_dependent::{*, shortcut_graph::CustomizedGraphReconstrctor},
    },
    shortest_path::{
        customizable_contraction_hierarchy::*,
        node_order::NodeOrder,
        query::{
            floating_td_customizable_contraction_hierarchy::Server,
            floating_td_dijkstra::Server as DijkServer
        },
    },
    io::*,
    benchmark::*,
    report::*,
};

use time::Duration;
use rand::prelude::*;

fn main() {
    // let _reporter = enable_reporting();

    report!("program", "tdcch");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());
    let seed = Default::default();
    report!("seed", seed);
    report!("num_threads", rayon::current_num_threads());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch_folder = path.join("cch");
    let node_order = NodeOrder::reconstruct_from(cch_folder.to_str().unwrap()).expect("could not read node order");
    let cch = CCHReconstrctor { original_graph: &graph, node_order }.reconstruct_from(cch_folder.to_str().unwrap()).expect("could not read cch");

    let customized_folder = path.join("customized");

    let td_cch_graph = CustomizedGraphReconstrctor { original_graph: &graph, first_out: cch.first_out(), head: cch.head() }.reconstruct_from(customized_folder.to_str().unwrap()).expect("could not read customized");

    let mut td_dijk_server = DijkServer::new(graph.clone());
    let mut server = Server::new(&cch, &td_cch_graph);

    let mut rng = StdRng::from_seed(seed);

    let mut rank_times = vec![Vec::new(); 64];

    for _ in 0..50 {
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let at = Timestamp::new(rng.gen_range(0.0, f64::from(period())));
        td_dijk_server.ranks(from, at, |to, ea_ground_truth, rank| {
            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (ea, duration) = measure(|| server.distance(from, to, at).map(|dist| dist + at));

            report!("from", from);
            report!("to", to);
            report!("departure_time", f64::from(at));
            report!("rank", rank);
            report!("ground_truth", f64::from(ea_ground_truth));
            report!("running_time_ms", duration.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            if let Some(ea) = ea {
                report!("earliest_arrival", f64::from(ea));
            }

            if !ea.unwrap_or(Timestamp::NEVER).fuzzy_eq(ea_ground_truth) {
                eprintln!("TDCCH ❌ Rel Err for rank {}: {}", rank, f64::from((ea.unwrap_or(Timestamp::NEVER) - at) / (ea_ground_truth - at)) - 1.0);
            }
            if cfg!(feature = "tdcch-approx") {
                assert!(!ea.unwrap_or(Timestamp::NEVER).fuzzy_lt(ea_ground_truth), "{} {} {:?}", from, to, at);
            } else {
                assert!(ea_ground_truth.fuzzy_eq(ea.unwrap_or(Timestamp::NEVER)), "{} {} {:?}", from, to, at);
            }
            if !cfg!(feature = "tdcch-approx") || ea.is_some() {
                let (path, unpacking_duration) = measure(|| server.path());
                report!("unpacking_running_time_ms", unpacking_duration.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
                rank_times[rank].push((duration, unpacking_duration));
                graph.check_path(path);
            }
        });
    }

    for (rank, rank_times) in rank_times.into_iter().enumerate() {
        let count = rank_times.len();
        if count > 0 {
            let (sum, sum_unpacking) = rank_times.into_iter().fold((Duration::zero(), Duration::zero()), |(acc1, acc2), (t1, t2)| (acc1 + t1, acc2 + t2));
            let avg = sum / count as i32;
            let avg_unpacking = sum_unpacking / count as i32;
            eprintln!("rank: {} - avg running time: {} - avg unpacking time: {}", rank, avg, avg_unpacking);
        }
    }

    let mut query_dir = None;
    let mut base_dir = Some(path);

    while let Some(base) = base_dir {
        if base.join("uniform_queries").exists() {
            query_dir = Some(base.join("uniform_queries"));
            break;
        } else {
            base_dir = base.parent();
        }
    }

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node").to_str().unwrap()).expect("could not read source node");
        let at = Vec::<u32>::load_from(path.join("source_time").to_str().unwrap()).expect("could not read source time");
        let to = Vec::load_from(path.join("target_node").to_str().unwrap()).expect("could not read target node");

        let num_queries = 50;

        let mut dijkstra_time = Duration::zero();
        let mut tdcch_time = Duration::zero();

        for ((from, to), at) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).take(num_queries) {
            let at = Timestamp::new(f64::from(at) / 1000.0);

            let dijkstra_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (ground_truth, time) = measure(|| {
                td_dijk_server.distance(from, to, at).map(|dist| dist + at)
            });
            report!("from", from);
            report!("to", to);
            report!("departure_time", f64::from(at));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            if let Some(ground_truth) = ground_truth {
                report!("earliest_arrival", f64::from(ground_truth));
            }
            drop(dijkstra_query_ctxt);

            dijkstra_time = dijkstra_time + time;

            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (ea, time) = measure(|| {
                server.distance(from, to, at).map(|dist| dist + at)
            });
            tdcch_time = tdcch_time + time;

            report!("from", from);
            report!("to", to);
            report!("departure_time", f64::from(at));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            if let Some(ground_truth) = ground_truth {
                report!("ground_truth", f64::from(ground_truth));
            }
            if let Some(ea) = ea {
                report!("earliest_arrival", f64::from(ea));
            }

            if !ea.unwrap_or(Timestamp::NEVER).fuzzy_eq(ground_truth.unwrap_or(Timestamp::NEVER)) {
                eprintln!("TDCCH ❌ Rel Err {}", f64::from((ea.unwrap_or(Timestamp::NEVER) - at) / (ground_truth.unwrap_or(Timestamp::NEVER) - at)) - 1.0);
            }

            if cfg!(feature = "tdcch-approx") {
                assert!(!ea.unwrap_or(Timestamp::NEVER).fuzzy_lt(ground_truth.unwrap_or(Timestamp::NEVER)), "{} {} {:?}", from, to, at);
            } else {
                assert!(ea.unwrap_or(Timestamp::NEVER).fuzzy_eq(ground_truth.unwrap_or(Timestamp::NEVER)));
            }
        }
        eprintln!("Dijkstra {}", dijkstra_time / (num_queries as i32));
        eprintln!("TDCCH {}", tdcch_time / (num_queries as i32));
    }
}
