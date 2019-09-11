use std::{
    env,
    path::Path,
};

use time::Duration;

use bmw_routing_engine::{
    graph::*,
    shortest_path::topocore::preprocess,
    io::Load,
    benchmark::*,
};

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");

    let from = Vec::load_from(path.join("test/source").to_str().unwrap()).expect("could not read source");
    let to = Vec::load_from(path.join("test/target").to_str().unwrap()).expect("could not read target");
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap()).expect("could not read travel_time_length");

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let mut topocore = report_time("topocore preprocessing", || {
        preprocess(&graph)
    });

    let mut total_query_time = Duration::zero();

    let num_queries = 100;

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(num_queries) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        let (res, time) = measure(|| {
            topocore.distance(from, to)
        });
        assert_eq!(res, ground_truth);

        total_query_time = total_query_time + time;
    }

    eprintln!("Avg. query time {}", total_query_time / (num_queries as i32));
}
