use std::env;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use import::here;
use import::here::csv_source::CSVSource;
use std::path::Path;
use io::Store;

fn main() {
    let mut args = env::args();
    args.next();

    let in_dir = &args.next().expect("No input directory given");
    let source = CSVSource::new(Path::new(in_dir));
    let (graph, lat, lng) = here::read_graph(&source);
    let out_dir = &args.next().expect("No output directory given");
    graph.write_to_dir(out_dir).expect("writing graph failed");
    lat.write_to(Path::new(out_dir).join("latitude").to_str().unwrap()).expect("writing lat failed");
    lng.write_to(Path::new(out_dir).join("longitude").to_str().unwrap()).expect("writing lng failed");
}
