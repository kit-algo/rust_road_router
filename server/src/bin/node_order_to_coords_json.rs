use std::env;

extern crate bmw_routing_engine;
#[macro_use]
extern crate serde_json;

use bmw_routing_engine::*;
use std::path::Path;
use io::read_into_vector;
use graph::*;


fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let lat: Vec<f32> = read_into_vector(path.join("latitude").to_str().unwrap()).expect("could not read first_out");
    let lng: Vec<f32> = read_into_vector(path.join("longitude").to_str().unwrap()).expect("could not read first_out");
    let ch_order: Vec<NodeId> = read_into_vector(path.join("travel_time_ch/order").to_str().unwrap()).expect("could not read travel_time_ch/order");

    let coords: Vec<serde_json::Value> = ch_order.iter().map(|&node| json!({ "lat": lat[node as usize], "lng": lng[node as usize] })).collect();
    let data = json!({ "coords": coords });
    println!("{}", data);
}
