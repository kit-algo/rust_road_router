/// Number of dijkstra queries performed for experiments.
/// Can be overriden through the NUM_DIJKSTRA_QUERIES env var.
pub fn num_dijkstra_queries() -> usize {
    std::env::var("NUM_DIJKSTRA_QUERIES").map_or(1000, |num| num.parse().unwrap())
}

use rand::{distributions::uniform::SampleUniform, prelude::*};
use time::Duration;

use crate::{
    algo::{dijkstra::*, *},
    datastr::graph::*,
    report::*,
};

pub mod catchup;
pub mod chpot;

pub struct ResultCallbackWrapper<S: QueryServer, F: for<'a> FnMut(&mut QueryResult<S::P<'a>, Weight>)>(pub std::marker::PhantomData<S>, pub F);

pub fn run_random_queries(
    num_nodes: usize,
    server: &mut impl QueryServer,
    rng: &mut StdRng,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
) {
    ResultCallbackWrapper(Default::default(), |_| ()).run_queries(
        std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
        server,
        Some(reporting_context),
        |_, _, _| (),
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
    ResultCallbackWrapper(Default::default(), |_| ()).run_queries(
        std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
        server,
        Some(reporting_context),
        pre_query,
        |_, _| None,
    );
}

impl<S: QueryServer, F: for<'a> FnMut(&mut QueryResult<S::P<'a>, Weight>)> ResultCallbackWrapper<S, F> {
    pub fn run_random_queries_with_callbacks(
        &mut self,
        num_nodes: usize,
        server: &mut S,
        rng: &mut StdRng,
        reporting_context: &mut CollectionContextGuard,
        num_queries: usize,
        pre_query: impl FnMut(NodeId, NodeId, &mut S),
        ground_truth: impl FnMut(NodeId, NodeId) -> Option<Option<Weight>>,
    ) {
        self.run_queries(
            std::iter::from_fn(move || Some((rng.gen_range(0..num_nodes as NodeId), rng.gen_range(0..num_nodes as NodeId)))).take(num_queries),
            server,
            Some(reporting_context),
            pre_query,
            ground_truth,
        );
    }

    pub fn run_queries(
        &mut self,
        query_iter: impl Iterator<Item = (NodeId, NodeId)>,
        server: &mut S,
        mut reporting_context: Option<&mut CollectionContextGuard>,
        mut pre_query: impl FnMut(NodeId, NodeId, &mut S),
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

            let (mut res, time) = measure(|| server.query(Query { from, to }));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            let dist = res.distance();
            report!("result", dist);

            if let Some(gt) = ground_truth(from, to) {
                assert_eq!(dist, gt);
            }

            (self.1)(&mut res);

            total_query_time = total_query_time + time;
        }

        if num_queries > 0 {
            eprintln!("Avg. query time {}", total_query_time / (num_queries as i32))
        };
    }
}

pub fn run_random_td_queries<
    T: Copy + serde::ser::Serialize + SampleUniform + Eq + PartialOrd,
    W: Copy + Eq + std::fmt::Debug + serde::ser::Serialize,
    S: TDQueryServer<T, W>,
>(
    num_nodes: usize,
    departure_range: std::ops::Range<T>,
    server: &mut S,
    rng: &mut StdRng,
    reporting_context: &mut CollectionContextGuard,
    num_queries: usize,
    pre_query: impl FnMut(NodeId, NodeId, &mut S),
    // with_result: impl for<'a> FnMut(&mut QueryResult<S::P<'a>, W>),
    ground_truth: impl FnMut(NodeId, NodeId) -> Option<Option<W>>,
) {
    run_td_queries(
        std::iter::from_fn(move || {
            Some((
                rng.gen_range(0..num_nodes as NodeId),
                rng.gen_range(0..num_nodes as NodeId),
                rng.gen_range(departure_range.clone()),
            ))
        })
        .take(num_queries),
        server,
        Some(reporting_context),
        pre_query,
        // with_result,
        ground_truth,
    );
}

pub fn run_td_queries<T: Copy + serde::ser::Serialize, W: Copy + Eq + std::fmt::Debug + serde::ser::Serialize, S: TDQueryServer<T, W>>(
    query_iter: impl Iterator<Item = (NodeId, NodeId, T)>,
    server: &mut S,
    mut reporting_context: Option<&mut CollectionContextGuard>,
    mut pre_query: impl FnMut(NodeId, NodeId, &mut S),
    // mut with_result: impl for<'a> FnMut(&mut QueryResult<S::P<'a>, W>),
    mut ground_truth: impl FnMut(NodeId, NodeId) -> Option<Option<W>>,
) {
    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut total_query_time = Duration::zero();
    let mut num_queries = 0;

    for (from, to, at) in query_iter {
        num_queries += 1;
        let _query_ctxt = reporting_context.as_mut().map(|ctxt| ctxt.push_collection_item());

        report!("from", from);
        report!("to", to);
        report!("at", at);

        pre_query(from, to, server);

        let (res, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        let dist = res.distance();
        report!("result", dist);

        if let Some(gt) = ground_truth(from, to) {
            assert_eq!(dist, gt);
        }

        // with_result(&mut res);

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

pub fn gen_many_to_many_queries(
    graph: &impl LinkIterGraph,
    num_queries: usize,
    ball_size: usize,
    set_size: usize,
    rng: &mut StdRng,
) -> Vec<(Vec<NodeId>, Vec<NodeId>)> {
    assert!(ball_size >= set_size);
    let n = graph.num_nodes();
    let mut queries = Vec::with_capacity(num_queries);

    let mut dijk_data = DijkstraData::new(n);
    let mut ops = DefaultOps();

    for _ in 0..num_queries {
        let source_center = rng.gen_range(0..n as NodeId);
        let dijk_run = DijkstraRun::query(
            graph,
            &mut dijk_data,
            &mut ops,
            Query {
                from: source_center,
                to: n as NodeId,
            },
        );
        let ball: Vec<_> = dijk_run.take(ball_size).collect();
        let sources = ball.choose_multiple(rng, set_size).copied().collect();

        // let target_center = rng.gen_range(0..n as NodeId);
        // let dijk_run = DijkstraRun::query(
        //     graph,
        //     &mut dijk_data,
        //     &mut ops,
        //     Query {
        //         from: target_center,
        //         to: n as NodeId,
        //     },
        // );
        // let ball: Vec<_> = dijk_run.take(ball_size).collect();
        let targets = ball.choose_multiple(rng, set_size).copied().collect();

        queries.push((sources, targets));
    }

    queries
}
