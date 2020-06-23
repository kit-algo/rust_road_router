#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{cli::CliErr, datastr::graph::*, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_weight_scaling");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    for factor in [
        1.0, 1.025, 1.05, 1.075, 1.1, 1.125, 1.15, 1.175, 1.2, 1.225, 1.25, 1.275, 1.3, 1.325, 1.35, 1.375, 1.4, 1.425, 1.45, 1.475, 1.5,
    ]
    .iter()
    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        bmw_routing_engine::experiments::chpot::run(path, |_graph, _rng, travel_time| {
            for weight in travel_time.iter_mut() {
                *weight = (*weight as f64 * factor) as Weight;
            }

            Ok(())
        })?;
    }

    Ok(())
}
