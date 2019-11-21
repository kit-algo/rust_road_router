use std::env;

use bmw_routing_engine::{export::*, graph::*, io::Load};
use std::path::Path;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let input_path = Path::new(arg);
    let arg = &args.next().expect("No directory arg given");
    let output_path = Path::new(arg);

    let first_out = Vec::load_from(input_path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(input_path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(input_path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    let graph = FirstOutGraph::new(first_out, head, travel_time);
    let lat = Vec::load_from(input_path.join("latitude").to_str().unwrap()).expect("could not read latitude");
    let lng = Vec::load_from(input_path.join("longitude").to_str().unwrap()).expect("could not read longitude");

    write_graph_to_gr(&graph, output_path.with_extension("gr").to_str().unwrap()).expect("could not write graph");
    write_coords_to_co(&lat, &lng, output_path.with_extension("co").to_str().unwrap()).expect("could not write coords");
}
