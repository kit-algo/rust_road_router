// Metric dependent part of CATCHUp preprocessing - the customization - with reporting for experiments.
// Takes as input one directory arg which should contain the all data and to which results will be written.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{
        graph::{floating_time_dependent::*, *},
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_customization");
    report!("num_threads", rayon::current_num_threads());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch_folder = path.join("cch");
    let node_order = NodeOrder::reconstruct_from(&cch_folder)?;
    let cch = CCHReconstrctor {
        original_graph: &graph,
        node_order,
    }
    .reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let _cch_customization_ctxt = algo_runs_ctxt.push_collection_item();
    let td_cch_graph = ftd_cch::customize(&cch, &graph);
    if !customized_folder.exists() {
        std::fs::create_dir(&customized_folder)?;
    }
    td_cch_graph.deconstruct_to(&customized_folder)?;

    Ok(())
}
