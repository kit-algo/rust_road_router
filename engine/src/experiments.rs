/// Number of dijkstra queries performed for experiments.
/// Can be overriden through the NUM_DIJKSTRA_QUERIES env var.
pub fn num_dijkstra_queries() -> usize {
    std::env::var("NUM_DIJKSTRA_QUERIES").map_or(1000, |num| num.parse().unwrap())
}

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
    run_queries(
        std::iter::from_fn(move || Some((rng.gen_range(0, num_nodes as NodeId), rng.gen_range(0, num_nodes as NodeId)))),
        server,
        reporting_context,
        num_queries,
    );
}

pub fn run_queries(
    query_iter: impl Iterator<Item = (NodeId, NodeId)>,
    server: &mut impl QueryServer,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
) {
    let mut total_query_time = Duration::zero();

    for (from, to) in query_iter.take(num_queries) {
        let _query_ctxt = reporting_context.push_collection_item();

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

pub fn rng(seed: <StdRng as SeedableRng>::Seed) -> StdRng {
    report!("seed", seed);
    StdRng::from_seed(seed)
}
