#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(rocket_codegen)]

#[macro_use]
extern crate serde_derive;

extern crate rocket;
extern crate rocket_contrib;
extern crate bmw_routing_engine;

use std::path::{Path, PathBuf};
use std::env;

use std::thread;
use std::sync::mpsc;
use mpsc::{Receiver, Sender};
use std::sync::Mutex;
// use std::sync::RwLock;

use std::cmp::Ordering;
use rocket::response::NamedFile;
use rocket::State;
use rocket_contrib::Json;

use bmw_routing_engine::*;
use graph::*;
use shortest_path::customizable_contraction_hierarchy;
use shortest_path::node_order::NodeOrder;
use shortest_path::query::customizable_contraction_hierarchy::Server;
use io::read_into_vector;
use bmw_routing_engine::benchmark::measure;

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

#[derive(Debug, FromForm, Copy, Clone)]
struct RoutingQuery {
    from_lat: f32,
    from_lng: f32,
    to_lat: f32,
    to_lng: f32
}

#[derive(Debug, Serialize, Deserialize)]
struct RoutingQueryResponse {
    distance: Weight,
    path: Vec<(f32, f32)>
}

#[get("/")]
fn index() -> Option<NamedFile> {
    NamedFile::open(Path::new("static/index.html")).ok()
}

#[get("/<file..>")]
fn files(file: PathBuf) -> Option<NamedFile> {
    NamedFile::open(Path::new("static/").join(file)).ok()
}

#[get("/query?<query_params>", format = "application/json")]
fn query(query_params: RoutingQuery, state: State<Mutex<(Sender<RoutingQuery>, Receiver<Option<RoutingQueryResponse>>)>>) -> Json<Option<RoutingQueryResponse>> {
    let result = measure("Total Query Request Time", || {
        println!("Received Query: {:?}", query_params);

        let (ref tx_query, ref rx_result) = *state.lock().unwrap();
        tx_query.send(query_params).unwrap();
        rx_result.recv().expect("routing engine crashed or hung up")
    });

    println!("");
    Json(result)
}

fn main() {
    let (tx_query, rx_query) = mpsc::channel::<RoutingQuery>();
    let (tx_result, rx_result) = mpsc::channel::<Option<RoutingQueryResponse>>();

    thread::spawn(move || {
        let mut args = env::args();
        args.next();

        let arg = &args.next().expect("No directory arg given");
        let path = Path::new(arg);

        let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
        let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
        let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

        let lat = read_into_vector(path.join("latitude").to_str().unwrap()).expect("could not read first_out");
        let lng = read_into_vector(path.join("longitude").to_str().unwrap()).expect("could not read first_out");

        let graph = FirstOutGraph::new(first_out, head, travel_time);
        let cch_order = read_into_vector(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

        let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));

        let mut server = Server::new(&cch, &graph);

        let coords = |node: NodeId| -> (f32, f32) {
            (lat[node as usize], lng[node as usize])
        };

        let closest_node = |(p_lat, p_lng): (f32, f32)| -> NodeId {
            lat.iter().zip(lng.iter()).enumerate().min_by_key(|&(_, (lat, lon))| {
                NonNan::new(((lat - p_lat) * (lat - p_lat) + (lon - p_lng) * (lon - p_lng)).sqrt())
            }).map(|(id, _)| id).unwrap() as NodeId
        };

        loop {
            match rx_query.recv() {
                Ok(query_params) => {
                    let RoutingQuery { from_lat, from_lng, to_lat, to_lng } = query_params;

                    let (from, to) = measure("match nodes", || {
                        (closest_node((from_lat, from_lng)), closest_node((to_lat, to_lng)))
                    });

                    let result = measure("cch query", || {
                        match server.distance(from, to) {
                            Some(distance) => {
                                let path = server.path().iter().map(|&node| coords(node)).collect();
                                Some(RoutingQueryResponse { distance, path })
                            },
                            None => None,
                        }
                    });

                    tx_result.send(result).unwrap();
                },
                Err(_) => panic!("query connection hung up"),
            }
        }
    });

    let config = rocket::config::Config::build(rocket::config::Environment::Staging)
        .port(8888)
        .finalize().expect("Could not create config");

    rocket::custom(config, false)
        .mount("/", routes![index, files, query])
        .manage(Mutex::new((tx_query, rx_result)))
        .launch();
}
