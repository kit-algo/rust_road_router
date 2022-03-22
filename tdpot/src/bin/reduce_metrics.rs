// WIP: CH potentials for TD Routing.

use rust_road_router::{algo::customizable_contraction_hierarchy::*, cli::CliErr, io::*, report::benchmark::report_time};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let target_num_metrics = args.next().map(|arg| arg.parse().unwrap()).unwrap_or(24);

    let customized_folder = path.join(args.next().unwrap_or("customized_corridor_mins".to_string()));
    let mut catchup = customization::ftd_for_pot::PotData::reconstruct_from(&customized_folder)?;

    let m = catchup.fw_static_bound.len();
    let metrics: Vec<_> = catchup.fw_bucket_bounds.chunks(m).collect();
    let num_buckets = metrics.len();
    assert_eq!(catchup.bucket_to_metric.len(), num_buckets);
    let merged = report_time("merging", || rust_road_router::algo::metric_merging::merge(&metrics, target_num_metrics));
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

    let customized_folder = path.join(args.next().unwrap_or("customized_corridor_mins".to_string()));
    catchup.deconstruct_to(&customized_folder)?;

    Ok(())
}
