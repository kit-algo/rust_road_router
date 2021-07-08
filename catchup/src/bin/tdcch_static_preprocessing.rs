// Metric independent parts of CATCHUp preprocessing with reporting for experiments.
// Takes as input one directory arg which should contain the graph and an order and to which the results will be written.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch_folder = path.join("cch");

    let cch_order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = contract(&graph, cch_order);
    drop(cch_build_ctxt);

    let latitude = Vec::<f32>::load_from(path.join("latitude"))?;
    let longitude = Vec::<f32>::load_from(path.join("longitude"))?;

    let cch_order = CCHReordering {
        cch: &cch,
        latitude: &latitude,
        longitude: &longitude,
    }
    .reorder();

    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = contract(&graph, cch_order);
    drop(cch_build_ctxt);

    let cch_order = CCHReordering {
        cch: &cch,
        latitude: &latitude,
        longitude: &longitude,
    }
    .reorder_for_seperator_based_customization();
    if !cch_folder.exists() {
        std::fs::create_dir(&cch_folder)?;
    }
    cch_order.deconstruct_to(&cch_folder)?;

    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = contract(&graph, cch_order);
    drop(cch_build_ctxt);

    cch.deconstruct_to(&cch_folder)?;

    Ok(())
}
