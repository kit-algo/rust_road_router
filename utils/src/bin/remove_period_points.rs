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

    let mut new_first_ipp_of_arc: Vec<u32> = Vec::with_capacity(graph.num_arcs() + 1);
    new_first_ipp_of_arc.push(0);
    let mut new_ipp_departure_time: Vec<Timestamp> = Vec::with_capacity(graph.num_ipps());
    let mut new_ipp_travel_time: Vec<Weight> = Vec::with_capacity(graph.num_ipps());

    for e in 0..graph.num_arcs() {
        let ttf = graph.travel_time_function(e as u32);
        new_ipp_departure_time.extend_from_slice(ttf.departure_time);
        new_ipp_travel_time.extend_from_slice(ttf.travel_time);

        if *new_ipp_departure_time.last().unwrap() == period() {
            new_ipp_departure_time.pop();
            new_ipp_travel_time.pop();
        }

        new_first_ipp_of_arc.push(new_ipp_travel_time.len() as u32);
    }

    new_first_ipp_of_arc.write_to(&path.join("first_ipp_of_arc"))?;
    new_ipp_departure_time.write_to(&path.join("ipp_departure_time"))?;
    new_ipp_travel_time.write_to(&path.join("ipp_travel_time"))?;

    Ok(())
}
