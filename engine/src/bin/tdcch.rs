use std::env;
use std::path::Path;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use graph::*;
use graph::time_dependent::*;
use shortest_path::customizable_contraction_hierarchy;
use shortest_path::node_order::NodeOrder;
use io::Load;
use bmw_routing_engine::benchmark::measure;
use shortest_path::query::time_dependent_customizable_contraction_hierarchy::Server;
use shortest_path::query::td_dijkstra::Server as DijkServer;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let mut first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::<Timestamp>::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::<Weight>::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), ipp_departure_time.len());

    // let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
    // let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

    // let mut added = 0;

    // for i in 0..head.len() {
    //     let range = first_ipp_of_arc[i] as usize .. first_ipp_of_arc[i+1] as usize;
    //     assert_ne!(range.start, range.end);

    //     first_ipp_of_arc[i] += added;

    //     if ipp_departure_time[range.start] == 0 {
    //         new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
    //         new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
    //         new_ipp_departure_time.push(period());
    //         new_ipp_travel_time.push(ipp_travel_time[range.start]);
    //         added += 1;
    //     } else if range.end - range.start >= 2 {
    //         if ipp_travel_time[range.start] != ipp_travel_time[range.end - 1] {
    //             println!("{:?} {:?}", &ipp_departure_time[range.clone()], &ipp_travel_time[range.clone()]);
    //         }
    //         new_ipp_departure_time.push(0);
    //         new_ipp_travel_time.push(ipp_travel_time[range.start]);
    //         new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
    //         new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
    //         new_ipp_departure_time.push(period());
    //         new_ipp_travel_time.push(ipp_travel_time[range.start]);
    //         added += 2;
    //     } else {
    //         new_ipp_departure_time.push(0);
    //         new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
    //         new_ipp_departure_time.push(period());
    //         new_ipp_travel_time.push(ipp_travel_time[range.start]);
    //         added += 1;
    //     }
    // }
    // first_ipp_of_arc[head.len()] += added;

    // println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), new_ipp_departure_time.len());
    // let graph = TDGraph::new(first_out, head, first_ipp_of_arc, new_ipp_departure_time, new_ipp_travel_time);

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

    let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));

    let td_cch_graph = cch.customize_td(&graph);
    println!("{:?}", td_cch_graph.total_num_segments());
    td_cch_graph.print_segment_stats();

    let mut td_dijk_server = DijkServer::new(graph.clone());
    let mut server = Server::new(&cch, &td_cch_graph);

    let from = Vec::load_from(path.join("uniform_queries/source_node").to_str().unwrap()).expect("could not read source node");
    let at = Vec::load_from(path.join("uniform_queries/source_time").to_str().unwrap()).expect("could not read source time");
    let to = Vec::load_from(path.join("uniform_queries/target_node").to_str().unwrap()).expect("could not read target node");
    let ground_truth = Vec::load_from(path.join("uniform_queries/day/di/optimal_target_time").to_str().unwrap()).expect("could not read travel_time_length");

    for (((from, to), at), ground_truth) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).zip(ground_truth.into_iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        measure("TD Dijkstra query", || {
            assert_eq!(td_dijk_server.distance(from, to, at).map(|dist| dist + at), ground_truth);
        });

        measure("TDCCH query", || {
            let dist = server.distance(from, to, at).map(|dist| dist + at);
            if dist == ground_truth {
                println!("✅ {:?} {:?}", dist, ground_truth);
            } else {
                println!("❌ {:?} {:?}", dist, ground_truth);
            }
            // assert_eq!(server.distance(from, to, at).map(|dist| dist + at), ground_truth);
        });
    }
}
