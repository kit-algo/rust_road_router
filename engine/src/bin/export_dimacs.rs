// Utility to convert a graph from RoutingKit format to DIMACs format

use std::{env, error::Error, path::Path};

use rust_road_router::{cli::CliErr, datastr::graph::*, export::*, io::Load};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let input_path = Path::new(arg);
    let arg = &args.next().ok_or(CliErr("No output directory arg given"))?;
    let output_path = Path::new(arg);

    let first_out = Vec::load_from(input_path.join("first_out"))?;
    let head = Vec::load_from(input_path.join("head"))?;
    let travel_time = Vec::load_from(input_path.join("travel_time"))?;
    let graph = FirstOutGraph::new(first_out, head, travel_time);
    let lat = Vec::load_from(input_path.join("latitude"))?;
    let lng = Vec::load_from(input_path.join("longitude"))?;

    write_graph_to_gr(&graph, output_path.with_extension("gr").to_str().unwrap())?;
    write_coords_to_co(&lat, &lng, output_path.with_extension("co").to_str().unwrap())?;

    Ok(())
}
