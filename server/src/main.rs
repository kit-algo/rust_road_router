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
use std::sync::RwLock;
use std::cmp::Ordering;
use rocket::response::NamedFile;
use rocket::State;
use rocket_contrib::Json;

use bmw_routing_engine::*;
use graph::first_out_graph::FirstOutGraph as Graph;
use graph::*;
use io::read_into_vector;

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

#[derive(Debug)]
struct RoutingWrapper {
    server: shortest_path::query::bidirectional_dijkstra::Server<Graph, Graph, Box<Graph>>,
    lat: Vec<f32>,
    lng: Vec<f32>
}

impl RoutingWrapper {
    fn coords(&self, node: NodeId) -> (f32, f32) {
        (self.lat[node as usize], self.lng[node as usize])
    }

    fn closest_node(&self, (p_lat, p_lng): (f32, f32)) -> NodeId {
        self.lat.iter().zip(self.lng.iter()).enumerate().min_by_key(|&(_, (lat, lon))| {
            NonNan::new(((lat - p_lat) * (lat - p_lat) + (lon - p_lng) * (lon - p_lng)).sqrt())
        }).map(|(id, _)| id).unwrap() as NodeId
    }
}

#[derive(Debug, FromForm)]
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
fn query(query_params: RoutingQuery, state: State<RwLock<RoutingWrapper>>) -> Json<Option<RoutingQueryResponse>> {
    let mut wrapper = state.write().unwrap();
    let RoutingQuery { from_lat, from_lng, to_lat, to_lng } = query_params;
    let from = wrapper.closest_node((from_lat, from_lng));
    let to = wrapper.closest_node((to_lat, to_lng));

    match wrapper.server.distance(from, to) {
        Some(distance) => {
            let path = wrapper.server.path().iter().map(|&node| wrapper.coords(node)).collect();
            Json(Some(RoutingQueryResponse { distance, path }))
        },
        None => Json(None)
    }
}

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    let graph = Graph::new(first_out, head, travel_time);

    let lat = read_into_vector(path.join("latitude").to_str().unwrap()).expect("could not read first_out");
    let lng = read_into_vector(path.join("longitude").to_str().unwrap()).expect("could not read first_out");

    let wrapper = RoutingWrapper {
        server: shortest_path::query::bidirectional_dijkstra::Server::new(Box::new(graph)), lat, lng
    };

    let config = rocket::config::Config::build(rocket::config::Environment::Staging)
        .port(8888)
        .finalize().expect("Could not create config");

    rocket::custom(config, false)
        .mount("/", routes![index, files, query])
        .manage(RwLock::new(wrapper))
        .launch();
}
