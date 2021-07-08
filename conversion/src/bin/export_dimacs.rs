// Utility to convert a graph from RoutingKit format to DIMACs format

use std::{env, error::Error, path::Path};

use rust_road_router::{cli::CliErr, datastr::graph::*, export::*, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);

    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let input_path = Path::new(arg);
    let arg = &args.next().ok_or(CliErr("No output directory arg given"))?;
    let output_path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&input_path)?;
    let lat = Vec::load_from(input_path.join("latitude"))?;
    let lng = Vec::load_from(input_path.join("longitude"))?;

    write_graph_to_gr(&graph, output_path.with_extension("gr").to_str().unwrap())?;
    write_coords_to_co(&lat, &lng, output_path.with_extension("co").to_str().unwrap())?;

    Ok(())
}
