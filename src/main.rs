extern crate bmw_routing_engine;
extern crate svg;

use std::process::Command;
use std::env;
use std::path::Path;

use bmw_routing_engine::*;
use graph::first_out_graph::FirstOutGraph as Graph;
use graph::*;
use shortest_path::DijkstrableGraph;
use io::read_into_vector;

use svg::Document;
use svg::node::element::Line;

use std::cmp::Ordering;

#[derive(PartialEq,PartialOrd)]
struct NonNan(f32);

impl NonNan {
    fn new(val: f32) -> Option<NonNan> {
        if val.is_nan() {
            None
        } else {
            Some(NonNan(val))
        }
    }
}

impl Eq for NonNan {}

impl Ord for NonNan {
    fn cmp(&self, other: &NonNan) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let lat: Vec<f32> = read_into_vector(path.join("latitude").to_str().unwrap()).expect("could not read first_out");
    let lon: Vec<f32> = read_into_vector(path.join("longitude").to_str().unwrap()).expect("could not read first_out");
    let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    // let _ch_order = read_into_vector(path.join("travel_time_ch/order").to_str().unwrap()).expect("could not read travel_time_ch/order");

    let mut document = Document::new();
    let graph = Graph::new(first_out, head, travel_time);

    let min_lat = 48.992758;
    let max_lat = 49.043633;
    let min_lon = 8.340608;
    let max_lon = 8.489897;

    for node in 0..graph.num_nodes() {
        for Link { node: neighbor, .. } in graph.neighbor_iter(node as NodeId) {
            if (lat[node as usize] <= max_lat && lat[node as usize] >= min_lat && lon[node as usize] <= max_lon && lon[node as usize] >= min_lon)
                || (lat[neighbor as usize] <= max_lat && lat[neighbor as usize] >= min_lat && lon[neighbor as usize] <= max_lon && lon[neighbor as usize] >= min_lon) {
                let line = Line::new()
                    .set("x1", format!("{}", lon[node as usize]))
                    .set("y1", format!("{}", lat[node as usize]))
                    .set("x2", format!("{}", lon[neighbor as usize]))
                    .set("y2", format!("{}", lat[neighbor as usize]))
                    .set("class", "edge");

                document = document.add(line);
            }

        }
    }
    // let &min_lat = lat.iter().min_by_key(|&&num| NonNan::new(num)).unwrap();
    // let &max_lat = lat.iter().max_by_key(|&&num| NonNan::new(num)).unwrap();
    // let &min_lon = lon.iter().min_by_key(|&&num| NonNan::new(num)).unwrap();
    // let &max_lon = lon.iter().max_by_key(|&&num| NonNan::new(num)).unwrap();

    document = document
        .set("viewBox", (min_lon, min_lat, max_lon - min_lon, max_lat - min_lat))
        .set("transform", "scale(1,-1)");

    let output = &args.next().unwrap_or("image.svg".to_owned());
    svg::save(output, &document).unwrap();

    Command::new("sh")
        .arg("-c")
        .arg("echo '<?xml-stylesheet type=\"text/css\" href=\"style.css\"?>' > tmp")
        .output()
        .expect("failed to create tmp");

    Command::new("sh")
        .arg("-c")
        .arg(format!("cat {} >> tmp", output))
        .output()
        .expect("failed to cat");

    Command::new("sh")
        .arg("-c")
        .arg(format!("mv tmp {}", output))
        .output()
        .expect("failed to mv");
}
