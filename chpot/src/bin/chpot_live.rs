#[macro_use]
extern crate rust_road_router;
use rust_road_router::{cli::CliErr, datastr::graph::*, io::*, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_live");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let live_travel_time = Vec::<Weight>::load_from(path.join("live_travel_time"))?;

    rust_road_router::experiments::chpot::run(path, |_graph, _rng, query_weights| {
        let mut live_count = 0;

        for (query, input) in query_weights.iter_mut().zip(live_travel_time.iter()) {
            if input > query {
                *query = *input;
                live_count += 1;
            }
        }

        report!("live_traffic", { "num_applied": live_count });

        Ok(())
    })
}
