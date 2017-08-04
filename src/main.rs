use std::env;
use std::path::Path;
use std::time::Instant;

extern crate bmw_routing_engine;

use bmw_routing_engine::graph::Graph;
use bmw_routing_engine::graph::INFINITY;
use bmw_routing_engine::shortest_path::ShortestPathServer;
use bmw_routing_engine::io::read_into_vector;


fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let from = read_into_vector(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = read_into_vector(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = read_into_vector(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = Graph::new(first_out, head, travel_time);
    let mut server = ShortestPathServer::new(graph);

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };
        let now = Instant::now();
        assert_eq!(server.distance(from, to), ground_truth);
        println!("{:?}", now.elapsed());
    }
}
