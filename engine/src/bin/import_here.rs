use std::env;

extern crate bmw_routing_engine;

use bmw_routing_engine::{
    import::here::{
        read_graph,
        csv_source::CSVSource,
    },
    io::Store,
};
use std::path::Path;

fn main() {
    let mut args = env::args();
    args.next();

    let in_dir = &args.next().expect("No input directory given");
    let source = CSVSource::new(Path::new(in_dir));
    let (graph, lat, lng, link_id_mapping, here_rank_to_link_id) = read_graph(&source);
    let out_dir = &args.next().expect("No output directory given");
    graph.write_to_dir(out_dir).expect("writing graph failed");
    lat.write_to(Path::new(out_dir).join("latitude").to_str().unwrap()).expect("writing lat failed");
    lng.write_to(Path::new(out_dir).join("longitude").to_str().unwrap()).expect("writing lng failed");
    link_id_mapping.write_to(Path::new(out_dir).join("link_id_mapping").to_str().unwrap()).expect("writing link_id_mapping failed");
    here_rank_to_link_id.write_to(Path::new(out_dir).join("here_rank_to_link_id").to_str().unwrap()).expect("writing here_rank_to_link_id failed");
}
