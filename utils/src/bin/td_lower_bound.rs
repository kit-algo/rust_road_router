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

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time"))?;

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);
    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Vec<Weight>>();

    lower_bound.write_to(&path.join("lower_bound"))?;

    Ok(())
}
