#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{cli::CliErr, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_simple_scale");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    let _exp_ctx = exps_ctxt.push_collection_item();
    report!("experiment", "weight_scale");

    bmw_routing_engine::experiments::chpot::run(path, |_graph, _rng, travel_time| {
        for weight in travel_time.iter_mut() {
            *weight = *weight * 11 / 10;
        }

        Ok(())
    })?;

    Ok(())
}
