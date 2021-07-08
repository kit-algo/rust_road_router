// WIP: CH potentials for TD Routing.

use rust_road_router::{cli::CliErr, datastr::graph::time_dependent::*, datastr::graph::*, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);
    let graph = TDGraph::reconstruct_from(&path)?;

    // for edge in 0..graph.num_arcs() {
    //     let slope = graph.travel_time_function(edge as EdgeId).min_slope();
    //     if slope != 0.0 {
    //         eprintln!("{}", slope);
    //     }
    // }
    eprintln!(
        "{}",
        (0..graph.num_arcs())
            .map(|edge| graph.travel_time_function(edge as EdgeId).min_slope())
            .min_by(|x, y| x.partial_cmp(y).unwrap())
            .unwrap()
    );
    eprintln!(
        "{}",
        (0..graph.num_arcs())
            .map(|edge| graph.travel_time_function(edge as EdgeId).max_slope())
            .max_by(|x, y| x.partial_cmp(y).unwrap())
            .unwrap()
    );

    for e in 0..graph.num_arcs() {
        if graph.travel_time_function(e as EdgeId).min_slope() < -1.0 {
            eprintln!("{:?}", e);
        }
    }

    Ok(())
}
