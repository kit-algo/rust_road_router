#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{cli::CliErr, datastr::graph::*, io::*, report::*};
use std::{env, error::Error, path::Path};

const TUNNEL_BIT: u8 = 1;
const FREEWAY_BIT: u8 = 2;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_blocked");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let arc_category = Vec::<u8>::load_from(path.join("arc_category"))?;

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "perfect");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, _travel_time| Ok(()))?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "no_tunnels");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, travel_time| {
            for (weight, &category) in travel_time.iter_mut().zip(arc_category.iter()) {
                if (category & TUNNEL_BIT) != 0 {
                    *weight = INFINITY;
                }
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "no_highways");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, travel_time| {
            for (weight, &category) in travel_time.iter_mut().zip(arc_category.iter()) {
                if (category & FREEWAY_BIT) != 0 {
                    *weight = INFINITY;
                }
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "avoid_highways");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, travel_time| {
            for (weight, &category) in travel_time.iter_mut().zip(arc_category.iter()) {
                if (category & FREEWAY_BIT) != 0 {
                    *weight *= 3;
                }
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_increase");

        bmw_routing_engine::experiments::chpot::run(path, |_graph, travel_time| {
            for (weight, _category) in travel_time.iter_mut().zip(arc_category.iter()) {
                *weight = *weight * 21 / 20;
            }

            Ok(())
        })?;
    }

    Ok(())
}
