#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{cli::CliErr, datastr::graph::*, io::*, report::*};
use rand::seq::SliceRandom;
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_simulated_live");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let travel_time = Vec::<Weight>::load_from(path.join("travel_time"))?;

    let speed: Vec<_> = distance
        .iter()
        .zip(travel_time.iter())
        .map(|(&dist, &tt)| if tt == 0 { 0 } else { dist / tt })
        .collect();
    let max_speed = *speed.iter().max().unwrap();
    let mut max_speed_idxs = Vec::new();
    for (i, &speed) in speed.iter().enumerate() {
        if speed == max_speed {
            max_speed_idxs.push(i);
        }
    }

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "random_times_2");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, rng, travel_time| {
            for idx in rand::seq::index::sample(rng, travel_time.len(), 1000).iter() {
                travel_time[idx] *= 2;
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "random_times_10");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, rng, travel_time| {
            for idx in rand::seq::index::sample(rng, travel_time.len(), 1000).iter() {
                travel_time[idx] *= 10;
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "fastest_times_2");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, rng, travel_time| {
            max_speed_idxs.shuffle(rng);
            for &idx in max_speed_idxs.iter().take(1000) {
                travel_time[idx] *= 2;
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "fastest_times_10");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, rng, travel_time| {
            max_speed_idxs.shuffle(rng);
            for &idx in max_speed_idxs.iter().take(1000) {
                travel_time[idx] *= 10;
            }

            Ok(())
        })?;
    }

    Ok(())
}
