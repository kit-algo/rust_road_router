/// Number of dijkstra queries performed for experiments.
/// Can be overriden through the NUM_DIJKSTRA_QUERIES env var.
#[cfg(not(override_num_dijkstra_queries))]
pub const NUM_DIJKSTRA_QUERIES: usize = 1000;
#[cfg(override_num_dijkstra_queries)]
pub const NUM_DIJKSTRA_QUERIES: usize = include!(concat!(env!("OUT_DIR"), "/NUM_DIJKSTRA_QUERIES"));

use rand::prelude::*;
use time::Duration;

use crate::{algo::*, datastr::graph::*, report::*};

pub mod a_star;
pub mod catchup;
pub mod chpot;

pub fn run_random_queries(
    num_nodes: usize,
    server: &mut impl QueryServer,
    rng: &mut StdRng,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
) {
    let mut total_query_time = Duration::zero();

    for _ in 0..num_queries {
        let _query_ctxt = reporting_context.push_collection_item();
        let from: NodeId = rng.gen_range(0, num_nodes as NodeId);
        let to: NodeId = rng.gen_range(0, num_nodes as NodeId);

        report!("from", from);
        report!("to", to);

        let (mut res, time) = measure(|| server.query(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist);
        res.as_mut().map(|res| res.path());

        total_query_time = total_query_time + time;
    }

    if num_queries > 0 {
        eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
    };
}
