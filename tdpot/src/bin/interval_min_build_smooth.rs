use rust_road_router::{
    algo::{customizable_contraction_hierarchy::*, traffic_aware::td_traffic_pots::IntervalMinPotential},
    cli::CliErr,
    datastr::graph::*,
    datastr::node_order::*,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("interval_min_build");
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let pre_in = args.next().unwrap_or("customized_corridor_mins".to_string());
    let pot_out = args.next().unwrap_or("interval_min_pot".to_string());

    let graph = time_dependent::TDGraph::reconstruct_from(&path)?;
    let lower_bound: Vec<_> = (0..graph.num_arcs() as EdgeId).map(|e| graph.travel_time_function(e).lower_bound()).collect();
    let smooth_graph = BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound[..]);
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = report_time_with_key("cch_preprocessing", "cch_preprocessing", || CCH::fix_order_and_build(&graph, order));
    let catchup = customization::ftd_for_pot::PotData::reconstruct_from(&path.join(pre_in))?;
    let pot = report_time_with_key("build", "build", || {
        without_reporting(|| IntervalMinPotential::new(&cch, catchup, smooth_graph, &graph))
    });
    pot.deconstruct_to(&path.join(pot_out))?;

    Ok(())
}
