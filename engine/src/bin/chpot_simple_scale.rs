#[macro_use]
extern crate rust_road_router;
use rust_road_router::{cli::CliErr, datastr::graph::*, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_simple_scale");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    let _exp_ctx = exps_ctxt.push_collection_item();
    report!("experiment", "weight_scale");

    rust_road_router::experiments::chpot::run(path, |_graph, _rng, travel_time| {
        for weight in travel_time.iter_mut() {
            *weight = (*weight as f64 * 1.05) as Weight;
        }

        Ok(())
    })?;

    Ok(())
}
