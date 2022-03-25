// WIP: CH potentials for TD Routing.

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("interval_min_reduction");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let target_num_metrics = args.next().map(|arg| arg.parse().unwrap()).unwrap_or(24);
    let pre_in = args.next().unwrap_or("customized_corridor_mins".to_string());
    let pre_out = args.next().unwrap_or("customized_corridor_mins".to_string());
    let pot_out = args.next().unwrap_or("interval_min_pot".to_string());

    let mut catchup = customization::ftd_for_pot::PotData::reconstruct_from(&path.join(pre_in))?;

    let m = catchup.fw_static_bound.len();
    let metrics: Vec<_> = catchup.fw_bucket_bounds.chunks(m).collect();
    let num_buckets = metrics.len();
    assert_eq!(catchup.bucket_to_metric.len(), num_buckets);
    let merged = report_time_with_key("merge", "merge", || rust_road_router::algo::metric_merging::merge(&metrics, target_num_metrics));
    catchup.fw_bucket_bounds = merged
        .iter()
        .flat_map(|group| (0..m).map(|edge_idx| group.iter().map(|&metric_idx| metrics[metric_idx][edge_idx]).min().unwrap()))
        .collect();
    let metrics: Vec<_> = catchup.bw_bucket_bounds.chunks(m).collect();
    catchup.bw_bucket_bounds = merged
        .iter()
        .flat_map(|group| (0..m).map(|edge_idx| group.iter().map(|&metric_idx| metrics[metric_idx][edge_idx]).min().unwrap()))
        .collect();
    let mut bucket_to_metric = vec![0; num_buckets];
    for (metric_idx, buckets) in merged.into_iter().enumerate() {
        for bucket in buckets {
            bucket_to_metric[bucket] = metric_idx;
        }
    }
    catchup.bucket_to_metric = bucket_to_metric;

    catchup.deconstruct_to(&path.join(pre_out))?;

    let pot = report_time_with_key("build", "build", || {
        let _blocked = block_reporting();
        rust_road_router::algo::td_astar::IntervalMinPotential::new(&cch, catchup)
    });
    pot.deconstruct_to(&path.join(pot_out))?;

    Ok(())
}
