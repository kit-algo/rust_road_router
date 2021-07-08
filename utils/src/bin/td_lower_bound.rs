// Export lower bound weights from td graph.

use rust_road_router::{
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Vec<Weight>>();

    lower_bound.write_to(&path.join("lower_bound"))?;

    Ok(())
}
