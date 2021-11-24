#[macro_use]
extern crate rust_road_router;

use rand::prelude::*;
use rust_road_router::{
    algo::{ch_potentials::*, customizable_contraction_hierarchy::*, minimal_nonshortest_subpaths::*, traffic_aware::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::*,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("ubs_performance");
    let mut rng = rng(Default::default());
    let num_queries = chpot::num_queries();

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);
    let epsilon = args.next().map(|arg| arg.parse().expect("could not parse epsilon")).unwrap_or(1.0);
    report!("epsilon", epsilon);
    let live_weight_file = args.next().unwrap_or("live_travel_time".to_string());
    report!("live_weight_file", live_weight_file);

    let mut graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    for w in graph.weights_mut() {
        *w = std::cmp::max(1, *w);
    }

    let sources = Vec::<NodeId>::load_from(path.join("queries/rank/source"))?;
    let targets = Vec::<NodeId>::load_from(path.join("queries/rank/target"))?;
    let ranks = Vec::<u32>::load_from(path.join("queries/rank/target"))?;

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let smooth_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let mut modified_travel_time = Vec::<Weight>::load_from(path.join(live_weight_file))?;
    for (w, smooth) in modified_travel_time.iter_mut().zip(graph.weight().iter()) {
        if *w == 0 {
            *w = *smooth;
        }
    }
    let live_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);
    for &w in live_graph.weight() {
        assert!(w > 0);
    }

    let live_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &live_graph)
    };

    let num_queries = std::cmp::min(num_queries, sources.len());

    let mut rank_paths = vec![Vec::new(); *ranks.iter().max().unwrap() as usize];

    let mut server = HeuristicTrafficAwareServer::new(live_graph.clone(), &smooth_cch_pot, &live_cch_pot);
    for ((&from, &to), &rank) in sources.iter().zip(targets.iter()).zip(ranks.iter()).take(num_queries) {
        let _blocked = block_reporting();
        server.query(Query { from, to }, epsilon, |p| rank_paths[rank as usize].push(p.to_vec()));
    }

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    for paths in &mut rank_paths {
        paths.shuffle(&mut rng);
    }

    let mut ubs_checker = MinimalNonShortestSubPaths::new(&smooth_cch_pot);

    for (rank, paths) in rank_paths.iter().enumerate() {
        for p in paths {
            let _algo_run = algo_runs_ctxt.push_collection_item();
            report!("algo", "lazy_rphast_tree");
            report!("rank", rank);
            report!("num_nodes_on_path", p.len());
            let num_violating = silent_report_time(|| ubs_checker.find_ubs_violating_subpaths(p, epsilon).len());
            report!("num_violating_segments", num_violating);
        }
    }

    for (rank, paths) in rank_paths.iter().enumerate() {
        for p in paths {
            let _algo_run = algo_runs_ctxt.push_collection_item();
            report!("algo", "lazy_rphast_naive");
            report!("rank", rank);
            report!("num_nodes_on_path", p.len());
            let num_violating = silent_report_time(|| ubs_checker.find_ubs_violating_subpaths_lazy_rphast_naive(p, epsilon).len());
            report!("num_violating_segments", num_violating);
        }
    }

    let mut ubs_checker = MinimalNonShortestSubPathsSSERphast::new(&smooth_cch_pot);

    for (rank, paths) in rank_paths.iter().enumerate() {
        for p in paths {
            let _algo_run = algo_runs_ctxt.push_collection_item();
            report!("algo", "sse_rphast");
            report!("rank", rank);
            report!("num_nodes_on_path", p.len());
            let num_violating = silent_report_time(|| ubs_checker.find_ubs_violating_subpaths_sse_rphast(p, epsilon).len());
            report!("num_violating_segments", num_violating);
        }
    }

    let mut ubs_checker = MinimalNonShortestSubPathsDijkstra::new(graph.borrowed());

    for (rank, paths) in rank_paths.iter().enumerate() {
        for p in paths {
            let _algo_run = algo_runs_ctxt.push_collection_item();
            report!("algo", "dijkstra_tree");
            report!("rank", rank);
            report!("num_nodes_on_path", p.len());
            let num_violating = silent_report_time(|| ubs_checker.find_ubs_violating_subpaths(p, epsilon).len());
            report!("num_violating_segments", num_violating);
        }
    }

    Ok(())
}
