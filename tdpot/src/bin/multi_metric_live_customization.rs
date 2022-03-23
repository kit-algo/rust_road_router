use rust_road_router::{
    algo::{customizable_contraction_hierarchy::*, td_astar::*},
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("multi_metric_live_customization");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    let pre_in = args.next().unwrap_or("multi_metric_pre".to_string());
    let pot_out = args.next().unwrap_or("multi_metric_pot".to_string());

    let graph = time_dependent::TDGraph::reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let mut mmp: MultiMetricPreprocessed = cch.reconstruct_from(&path.join(pre_in))?;
    mmp.reserve_space_for_additional_metrics(1);

    let live_graph = (graph, live_data_file, t_live).reconstruct_from(&path)?;

    let multi_metric_pot = report_time_with_key("customization", "customization", || {
        let _blocked = block_reporting();
        mmp.customize_live(&live_graph, t_live);
        MultiMetric::new(mmp)
    });
    multi_metric_pot.deconstruct_to(&path.join(pot_out))?;

    Ok(())
}
