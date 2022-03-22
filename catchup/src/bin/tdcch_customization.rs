// Metric dependent part of CATCHUp preprocessing - the customization - with reporting for experiments.
// Takes as input one directory arg which should contain the all data and to which results will be written.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{algo::customizable_contraction_hierarchy::*, cli::CliErr, datastr::graph::floating_time_dependent::*, io::*, report::*};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_customization");
    report!("num_threads", rayon::current_num_threads());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let cch_folder = path.join("cch");
    let cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let _cch_customization_ctxt = algo_runs_ctxt.push_collection_item();
    let td_cch_graph = ftd_cch::customize(&cch, &graph);
    td_cch_graph.deconstruct_to(&customized_folder)?;

    Ok(())
}
