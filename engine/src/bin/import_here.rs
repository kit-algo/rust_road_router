use std::env;

extern crate bmw_routing_engine;

use bmw_routing_engine::{
    import::here::{csv_source::CSVSource, read_graph},
    io::Store,
};
use std::path::Path;

fn main() {
    let mut args = env::args();
    args.next();

    let in_dir = &args.next().expect("No input directory given");
    let out_dir = &args.next().expect("No output directory given");

    let min_lat = (args
        .next()
        .map(|s| s.parse::<f64>().unwrap_or_else(|_| panic!("could not parse {} as lat coord", s)))
        .unwrap_or(-360.0)
        * 100_000.) as i64;
    let min_lon = (args
        .next()
        .map(|s| s.parse::<f64>().unwrap_or_else(|_| panic!("could not parse {} as lon coord", s)))
        .unwrap_or(-360.0)
        * 100_000.) as i64;
    let max_lat = (args
        .next()
        .map(|s| s.parse::<f64>().unwrap_or_else(|_| panic!("could not parse {} as lat coord", s)))
        .unwrap_or(360.0)
        * 100_000.) as i64;
    let max_lon = (args
        .next()
        .map(|s| s.parse::<f64>().unwrap_or_else(|_| panic!("could not parse {} as lon coord", s)))
        .unwrap_or(360.0)
        * 100_000.) as i64;

    let source = CSVSource::new(Path::new(in_dir));
    let data = read_graph(&source, (min_lat, min_lon), (max_lat, max_lon));
    data.graph.write_to_dir(out_dir).expect("writing graph failed");
    data.functional_road_classes
        .write_to(Path::new(out_dir).join("functional_road_classes").to_str().unwrap())
        .expect("writing lat failed");
    data.lat
        .write_to(Path::new(out_dir).join("latitude").to_str().unwrap())
        .expect("writing lat failed");
    data.lng
        .write_to(Path::new(out_dir).join("longitude").to_str().unwrap())
        .expect("writing lng failed");
    data.link_id_mapping
        .write_to(Path::new(out_dir).join("link_id_mapping").to_str().unwrap())
        .expect("writing link_id_mapping failed");
    data.here_rank_to_link_id
        .write_to(Path::new(out_dir).join("here_rank_to_link_id").to_str().unwrap())
        .expect("writing here_rank_to_link_id failed");
}
