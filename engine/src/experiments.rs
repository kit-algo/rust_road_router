/// Number of dijkstra queries performed for experiments.
/// Can be overriden through the NUM_DIJKSTRA_QUERIES env var.
pub fn num_dijkstra_queries() -> usize {
    std::env::var("NUM_DIJKSTRA_QUERIES").map_or(1000, |num| num.parse().unwrap())
}

use rand::prelude::*;
use time::Duration;

use crate::{algo::*, datastr::graph::*, report::*};

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
        std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
        server,
        Some(reporting_context),
        |_, _, _| (),
        // |_| (),
        |_, _| None,
    );
}

pub fn run_random_queries_with_pre_callback<S: QueryServer>(
    num_nodes: usize,
    server: &mut S,
    rng: &mut StdRng,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
    pre_query: impl FnMut(NodeId, NodeId, &mut S),
) {
    run_queries(
        std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
        server,
        Some(reporting_context),
        pre_query,
        // |_| (),
        |_, _| None,
    );
}

pub fn run_random_queries_with_callbacks<S: QueryServer>(
    num_nodes: usize,
    server: &mut S,
    rng: &mut StdRng,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
    pre_query: impl FnMut(NodeId, NodeId, &mut S),
    // with_result: impl for<'a> FnMut(Option<&mut QueryResult<S::P<'a>, Weight>>),
    ground_truth: impl FnMut(NodeId, NodeId) -> Option<Option<Weight>>,
) {
    run_queries(
        std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
        server,
        Some(reporting_context),
        pre_query,
        // with_result,
        ground_truth,
    );
}

pub fn run_queries<S: QueryServer>(
    query_iter: impl Iterator<Item = (NodeId, NodeId)>,
    server: &mut S,
    mut reporting_context: Option<&mut CollectionContextGuard>,
    mut pre_query: impl FnMut(NodeId, NodeId, &mut S),
    // mut with_result: impl for<'a> FnMut(Option<&mut QueryResult<S::P<'a>, Weight>>),
    mut ground_truth: impl FnMut(NodeId, NodeId) -> Option<Option<Weight>>,
) {
    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut total_query_time = Duration::zero();
    let mut num_queries = 0;

    for (from, to) in query_iter {
        num_queries += 1;
        let _query_ctxt = reporting_context.as_mut().map(|ctxt| ctxt.push_collection_item());

        report!("from", from);
        report!("to", to);

        pre_query(from, to, server);

        let (res, time) = measure(|| server.query(Query { from, to }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.as_ref().map(|res| res.distance());
        report!("result", dist);

        if let Some(gt) = ground_truth(from, to) {
            assert_eq!(dist, gt);
        }

        // with_result(res.as_mut());

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
