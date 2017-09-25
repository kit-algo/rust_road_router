use std::env;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use import::here;
use import::here::csv_source::CSVSource;
use std::path::Path;


fn main() {
    let mut args = env::args();
    args.next();

    let dir = &args.next().expect("No input directory given");
    let source = CSVSource::new(Path::new(dir));
    let graph = here::read_graph(&source);
    println!("{:?}", graph);
}
