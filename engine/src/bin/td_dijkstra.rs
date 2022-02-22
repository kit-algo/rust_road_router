// TD Dijkstra baseline.

use rust_road_router::{
    algo::dijkstra::query::{dijkstra::Server, td_dijkstra::TDDijkstraOps},
    cli::CliErr,
    datastr::graph::time_dependent::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("td_dijkstra");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let mut server = Server::<_, TDDijkstraOps>::new(graph);

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

    affinity::set_thread_affinity(&[0]).unwrap();

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node"))?;
        let at = Vec::<u32>::load_from(path.join("source_time"))?;
        let to = Vec::load_from(path.join("target_node"))?;

        let iter = from.into_iter().zip(to.into_iter()).zip(at.into_iter()).map(|((s, t), a)| (s, t, a)).take(1000);
        experiments::run_td_queries(iter, &mut server, Some(&mut algo_runs_ctxt), |_, _, _, _| (), |_, _, _| None);
    }

    Ok(())
}
