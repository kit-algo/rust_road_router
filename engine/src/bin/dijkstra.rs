#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{dijkstra::*, *},
    cli::CliErr,
    datastr::graph::*,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

pub fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "dijkstra");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let seed = Default::default();
    report!("seed", seed);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let mut server = rust_road_router::algo::dijkstra::query::dijkstra::Server::<DefaultOps, _>::new(graph.clone());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut query_count = 0;
    let mut rng = StdRng::from_seed(seed);
    let mut total_query_time = Duration::zero();

    for _i in 0..rust_road_router::experiments::NUM_DIJKSTRA_QUERIES {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);

        report!("from", from);
        report!("to", to);

        query_count += 1;

        let (dist, time) = measure(|| QueryServer::query(&mut server, Query { from, to }).map(|res| res.distance()));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("result", dist.unwrap_or(INFINITY));
        total_query_time = total_query_time + time;
    }

    if query_count > 0 {
        eprintln!("Avg. query time {}", total_query_time / (query_count as i32))
    };

    Ok(())
}
