#![feature(slice_patterns)]

extern crate bmw_routing_engine;
extern crate svg;
extern crate rand;
extern crate getopts;

use std::process::Command;
use std::env;
use std::path::Path;
use std::cmp::max;
use std::cmp::Ordering;
use std::rc::Rc;

use bmw_routing_engine::*;
use graph::first_out_graph::FirstOutGraph as Graph;
use graph::*;
use shortest_path::DijkstrableGraph;
use shortest_path::query::bidirectional_dijkstra::Server as DijkServer;
use io::read_into_vector;

use svg::Document;
use svg::node::element::{Line, Circle};

use getopts::Options;

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

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} GRAPH_DIR [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("o", "output", "set output file name", "NAME");
    opts.optopt("s", "source", "source node position", "LAT,LNG");
    opts.optopt("t", "target", "target node position", "LAT,LNG");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let path = if !matches.free.is_empty() {
        Path::new(&matches.free[0])
    } else {
        print_usage(&program, opts);
        return;
    };
    let output = matches.opt_str("o").unwrap_or("image.svg".to_owned());

    let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let lat: Vec<f32> = read_into_vector(path.join("latitude").to_str().unwrap()).expect("could not read first_out");
    let lon: Vec<f32> = read_into_vector(path.join("longitude").to_str().unwrap()).expect("could not read first_out");
    let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let graph = Rc::new(Graph::new(first_out, head, travel_time));
    let mut query_server = DijkServer::new(graph.clone());

    let find_closest = |(p_lat, p_lon): (f32, f32)| -> NodeId {
        lat.iter().zip(lon.iter()).enumerate().min_by_key(|&(_, (lat, lon))| {
            NonNan::new(((lat - p_lat) * (lat - p_lat) + (lon - p_lon) * (lon - p_lon)).sqrt())
        }).map(|(id, _)| id).unwrap() as NodeId
    };

    let select_node = |arg: Option<String>| -> NodeId {
        match arg {
            Some(coord) => {
                let coords: Vec<f32> = coord.split(",").map(|val| val.parse::<f32>().expect(&format!("could not parse {}", val))).collect();
                match coords.as_slice() {
                    &[lat, lon] => find_closest((lat, lon)),
                    _ => panic!("wrong number of dimensions in {}", coord),
                }

            },
            None => rand::random::<NodeId>() % (graph.num_nodes() as NodeId),
        }
    };

    let s = select_node(matches.opt_str("s"));
    let t = select_node(matches.opt_str("t"));
    query_server.distance(s, t);

    let mut min_lat = lat.iter().enumerate().filter(|&(i, _)| query_server.is_in_searchspace(i as NodeId) ).map(|(_, &num)| num).min_by_key(|&num| NonNan::new(num)).unwrap();
    let mut max_lat = lat.iter().enumerate().filter(|&(i, _)| query_server.is_in_searchspace(i as NodeId) ).map(|(_, &num)| num).max_by_key(|&num| NonNan::new(num)).unwrap();
    let mut min_lon = lon.iter().enumerate().filter(|&(i, _)| query_server.is_in_searchspace(i as NodeId) ).map(|(_, &num)| num).min_by_key(|&num| NonNan::new(num)).unwrap();
    let mut max_lon = lon.iter().enumerate().filter(|&(i, _)| query_server.is_in_searchspace(i as NodeId) ).map(|(_, &num)| num).max_by_key(|&num| NonNan::new(num)).unwrap();

    let delta_lat = max_lat - min_lat;
    min_lat -= max(NonNan::new(delta_lat * 0.1), NonNan::new(0.001)).unwrap().0;
    max_lat += max(NonNan::new(delta_lat * 0.1), NonNan::new(0.001)).unwrap().0;
    let delta_lon = max_lon - min_lon;
    min_lon -= max(NonNan::new(delta_lon * 0.1), NonNan::new(0.001)).unwrap().0;
    max_lon += max(NonNan::new(delta_lon * 0.1), NonNan::new(0.001)).unwrap().0;

    let mut document = Document::new();

    for node in 0..graph.num_nodes() {
        let node_in_searchspace = query_server.is_in_searchspace(node as NodeId);

        for Link { node: neighbor, .. } in graph.neighbor_iter(node as NodeId) {
            if (lat[node as usize] <= max_lat && lat[node as usize] >= min_lat && lon[node as usize] <= max_lon && lon[node as usize] >= min_lon)
                || (lat[neighbor as usize] <= max_lat && lat[neighbor as usize] >= min_lat && lon[neighbor as usize] <= max_lon && lon[neighbor as usize] >= min_lon) {
                let mut line = Line::new()
                    .set("x1", format!("{}", lon[node as usize]))
                    .set("y1", format!("{}", lat[node as usize]))
                    .set("x2", format!("{}", lon[neighbor as usize]))
                    .set("y2", format!("{}", lat[neighbor as usize]))
                    .set("class", "edge");

                if node_in_searchspace && query_server.is_in_searchspace(neighbor) {
                    line = line.set("class", "searchspace");
                }

                document = document.add(line);
            }
        }
    }

    let mut prev = None;
    for &node in query_server.path().iter() {
        match prev {
            Some(prev_node) => {
                let line = Line::new()
                    .set("x1", format!("{}", lon[node as usize]))
                    .set("y1", format!("{}", lat[node as usize]))
                    .set("x2", format!("{}", lon[prev_node as usize]))
                    .set("y2", format!("{}", lat[prev_node as usize]))
                    .set("class", "shortest_path");

                document = document.add(line);
            },
            None => (),
        }
        prev = Some(node);
    }

    let source_circle = Circle::new()
        .set("cx", lon[s as usize])
        .set("cy", lat[s as usize])
        .set("r", 0.0007)
        .set("class", "marker source");
    document = document.add(source_circle);
    let target_circle = Circle::new()
        .set("cx", lon[t as usize])
        .set("cy", lat[t as usize])
        .set("r", 0.0007)
        .set("class", "marker target");
    document = document.add(target_circle);

    document = document
        .set("viewBox", (min_lon, min_lat, max_lon - min_lon, max_lat - min_lat))
        .set("transform", "scale(1,-1)");

    svg::save(&output, &document).unwrap();

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
