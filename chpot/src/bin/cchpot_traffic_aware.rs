#[macro_use]
extern crate rust_road_router;

use rand::prelude::*;
use rust_road_router::{
    algo::{ch_potentials::*, customizable_contraction_hierarchy::*, traffic_aware::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::{chpot::FakeTraffic, *},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};
use time::Duration;

#[allow(unused_braces)]
fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("traffic_aware");
    let mut rng = rng(Default::default());
    let mut modify_rng = rng.clone();
    let q = chpot::num_queries();

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    for w in graph.weights_mut() {
        *w = std::cmp::max(1, *w);
    }
    let n = graph.num_nodes();
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0];
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0];

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let smooth_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let mut modified_travel_time = graph.weight().to_vec();
    FakeTraffic::new(tt_units_per_s, dist_units_per_m, 30.0, 0.005, 5.0).simulate(&mut modify_rng, &mut modified_travel_time, &geo_distance);
    let live_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);
    for &w in live_graph.weight() {
        assert!(w > 0);
    }

    let live_cch_pot = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &live_graph)
    };

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let mut server = TrafficAwareServer::new(graph.borrowed(), live_graph, &smooth_cch_pot, &live_cch_pot);

    let mut total_query_time = Duration::zero();

    for _i in 0..q {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0..n as NodeId);
        let to: NodeId = rng.gen_range(0..n as NodeId);

        eprintln!();
        report!("from", from);
        report!("to", to);

        let (_, time) = measure(|| server.query(Query { from, to }, 0.25));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        eprintln!();

        total_query_time = total_query_time + time;
    }

    if q > 0 {
        eprintln!("Avg. query time {}", total_query_time / (q as i32))
    };

    Ok(())
}
