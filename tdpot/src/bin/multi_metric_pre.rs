use rust_road_router::{
    algo::{customizable_contraction_hierarchy::*, td_astar::*},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("multi_metric_pre");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let pre_out = args.next().unwrap_or("multi_metric_pre".to_string());
    let pot_out = args.next().unwrap_or("multi_metric_pot".to_string());
    let num_metrics = args.next().map(|arg| arg.parse().unwrap());

    let graph = time_dependent::TDGraph::reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let mmp = report_time_with_key("preprocessing", "preprocessing", || {
        let _blocked = block_reporting();
        MultiMetricPreprocessed::new(&cch, ranges(), &graph, num_metrics)
    });
    mmp.deconstruct_to(&path.join(pre_out))?;

    let multi_metric_pot = report_time_with_key("build", "build", || {
        let _blocked = block_reporting();
        MultiMetric::new(mmp)
    });
    multi_metric_pot.deconstruct_to(&path.join(pot_out))?;

    Ok(())
}
