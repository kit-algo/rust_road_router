use std::{env, error::Error, path::Path, sync::Arc};

use bmw_routing_engine::{
    benchmark::report_time,
    cli::CliErr,
    graph::{first_out_graph::OwnedGraph as Graph, *},
    io::Load,
    shortest_path::{
        contraction_hierarchy,
        query::{bidirectional_dijkstra::Server as BiDijkServer, contraction_hierarchy::Server as CHServer, dijkstra::Server as DijkServer},
    },
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Arc::new(Vec::load_from(path.join("first_out").to_str().unwrap())?);
    let head = Arc::new(Vec::load_from(path.join("head").to_str().unwrap())?);
    let travel_time = Arc::new(Vec::load_from(path.join("travel_time").to_str().unwrap())?);

    let from = Vec::load_from(path.join("test/source").to_str().unwrap())?;
    let to = Vec::load_from(path.join("test/target").to_str().unwrap())?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length").to_str().unwrap())?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let mut simple_server = DijkServer::new(graph.clone());
    let mut bi_dir_server = BiDijkServer::new(graph.clone());

    let ch_first_out = Vec::load_from(path.join("travel_time_ch/first_out").to_str().unwrap())?;
    let ch_head = Vec::load_from(path.join("travel_time_ch/head").to_str().unwrap())?;
    let ch_weight = Vec::load_from(path.join("travel_time_ch/weight").to_str().unwrap())?;
    let ch_order = Vec::load_from(path.join("travel_time_ch/order").to_str().unwrap())?;
    let mut inverted_order = vec![0; ch_order.len()];
    for (i, &node) in ch_order.iter().enumerate() {
        inverted_order[node as usize] = i as u32;
    }
    let mut ch_server = CHServer::new((Graph::new(ch_first_out, ch_head, ch_weight).ch_split(&inverted_order), None));
    let mut ch_server_with_own_ch = CHServer::new(contraction_hierarchy::contract(&graph, ch_order));

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        report_time("simple dijkstra", || {
            assert_eq!(simple_server.distance(from, to), ground_truth);
        });
        report_time("bidir dijkstra", || {
            assert_eq!(bi_dir_server.distance(from, to), ground_truth);
        });
        report_time("CH", || {
            assert_eq!(ch_server.distance(from, to), ground_truth);
        });
        report_time("own CH", || {
            assert_eq!(
                ch_server_with_own_ch.distance(inverted_order[from as usize], inverted_order[to as usize]),
                ground_truth
            );
        });
    }

    Ok(())
}
