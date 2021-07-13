#[macro_use]
extern crate rust_road_router;
use rust_road_router::{cli::CliErr, datastr::graph::*, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_weight_scaling");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    for factor in [
        1., 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.1, 1.11, 1.12, 1.13, 1.14, 1.15, 1.16, 1.17, 1.18, 1.19, 1.2, 1.21, 1.22, 1.23, 1.24, 1.25,
    ]
    .iter()
    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        rust_road_router::experiments::chpot::run(path, |_graph, _rng, travel_time| {
            for weight in travel_time.iter_mut() {
                *weight = (*weight as f64 * factor) as Weight;
            }

            Ok(())
        })?;
    }

    Ok(())
}
