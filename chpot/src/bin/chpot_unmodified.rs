#[macro_use]
extern crate rust_road_router;
use rust_road_router::{cli::CliErr, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_unmodified");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments");

    let _exp_ctx = exps_ctxt.push_collection_item();
    report!("experiment", "perfect");

    rust_road_router::experiments::chpot::run(path, |_graph, _rng, _travel_time| Ok(()))?;

    Ok(())
}
