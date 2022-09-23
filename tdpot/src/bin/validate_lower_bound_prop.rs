#[macro_use]
extern crate rust_road_router;
use rust_road_router::{cli::CliErr, datastr::graph::*, io::*, report::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("live_and_predicted_queries");

    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let t_live = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    report!("t_live", t_live);
    report!("live_data_file", live_data_file);

    let live_graph = (live_data_file, t_live).reconstruct_from(&path)?;

    // live_graph.check_global_lower();

    let mut live_counter = 0;
    let mut valid_counter = 0;

    for e in 0..live_graph.num_arcs() {
        if let Some(valid) = live_graph.is_live_slower(e as EdgeId, t_live) {
            live_counter += 1;
            if valid {
                valid_counter += 1
            }
        }
    }

    report!("valid_edges", valid_counter);
    report!("live_edges", live_counter);
    report!("total_edges", live_graph.num_arcs());

    Ok(())
}
