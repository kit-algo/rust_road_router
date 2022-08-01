use rust_road_router::{
    algo::{customizable_contraction_hierarchy::*, traffic_aware::td_traffic_pots::*},
    cli::CliErr,
    datastr::graph::*,
    datastr::node_order::*,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("interval_min_live_customization");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    let pre_in = args.next().unwrap_or("customized_corridor_mins".to_string());
    let pot_out = args.next().unwrap_or("interval_min_pot".to_string());

    let live_graph = (live_data_file, t_live).reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(live_graph.graph(), order);

    let lower_bound: Vec<_> = (0..live_graph.num_arcs() as EdgeId)
        .map(|e| live_graph.graph().travel_time_function(e).lower_bound())
        .collect();
    let smooth_graph = BorrowedGraph::new(live_graph.graph().first_out(), live_graph.graph().head(), &lower_bound[..]);

    let catchup = customization::ftd_for_pot::PotData::reconstruct_from(&path.join(pre_in))?;

    let multi_metric_pot = report_time_with_key("customization", "customization", || {
        let _blocked = block_reporting();
        IntervalMinPotential::new_for_simple_live(&cch, catchup, &live_graph, t_live, smooth_graph)
    });
    multi_metric_pot.deconstruct_to(&path.join(pot_out))?;

    Ok(())
}
