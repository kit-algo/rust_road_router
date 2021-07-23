// Metric dependent part of CATCHUp preprocessing - the customization - with reporting for experiments.
// Takes as input one directory arg which should contain the all data and to which results will be written.

use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{catchup::light::*, customizable_contraction_hierarchy::*},
    cli::CliErr,
    datastr::graph::{time_dependent::*, Graph},
    experiments,
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("catchup_zero_customization");
    report!("num_threads", rayon::current_num_threads());
    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch_folder = path.join("cch");
    let cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let _cch_customization_ctxt = algo_runs_ctxt.push_collection_item();
    let customized = catchup_light::customize(&cch, &graph);
    drop(_cch_customization_ctxt);

    let mut server = Server::new(&cch, &customized);

    experiments::run_random_td_queries(
        n,
        0..period() as Timestamp,
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        1000,
        |_, _, _| (),
        |_, _| None,
    );

    Ok(())
}
