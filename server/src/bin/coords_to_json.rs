use std::env;

extern crate bmw_routing_engine;
#[macro_use]
extern crate serde_json;

use bmw_routing_engine::*;
use io::*;

fn main() {
    let mut args = env::args();
    args.next();

    let lat: Vec<f32> = Vec::load_from(&args.next().expect("No latitude arg given")).expect("could not read latitude");
    let lng: Vec<f32> = Vec::load_from(&args.next().expect("No longitude arg given")).expect("could not read longitude");
    assert!(lat.len() == lng.len());

    let coords: Vec<serde_json::Value> = lat.iter().zip(lng.iter()).map(|(lat, lng)| json!({ "lat": lat, "lng": lng })).collect();
    let data = json!({ "coords": coords });
    println!("{}", data);
}
