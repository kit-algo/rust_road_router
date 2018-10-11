use std::env;
use std::path::Path;

use bmw_routing_engine::{
    graph::{
        *,
        floating_time_dependent::*,
        time_dependent::period as int_period,
    },
    shortest_path::{
        customizable_contraction_hierarchy,
        node_order::NodeOrder,
        query::{
            time_dependent_customizable_contraction_hierarchy::Server,
            td_dijkstra::Server as DijkServer
        },
    },
    io::Load,
    benchmark::*,
};

use time::Duration;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let mut first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), ipp_departure_time.len());

    let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
    let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

    let mut added = 0;

    for i in 0..head.len() {
        let range = first_ipp_of_arc[i] as usize .. first_ipp_of_arc[i+1] as usize;
        assert_ne!(range.start, range.end);

        first_ipp_of_arc[i] += added;

        if ipp_departure_time[range.start] == 0 {
            new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
            new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
            new_ipp_departure_time.push(int_period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 1;
        } else if range.end - range.start > 1 {
            new_ipp_departure_time.push(0);
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
            new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
            new_ipp_departure_time.push(int_period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 2;
        } else {
            new_ipp_departure_time.push(0);
            new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
            new_ipp_departure_time.push(int_period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 1;
        }
    }
    first_ipp_of_arc[head.len()] += added;

    println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), new_ipp_departure_time.len());

    let points = new_ipp_departure_time.into_iter().zip(new_ipp_travel_time.into_iter()).map(|(dt, tt)| {
        Point { at: Timestamp::new(f64::from(dt) / 1000.0), val: FlWeight::new(f64::from(tt) / 1000.0) }
    }).collect();

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, points);

    // let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");

    let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));

    let td_cch_graph = cch.customize_floating_td(&graph);
    // println!("{:?}", td_cch_graph.total_num_segments());
    // td_cch_graph.print_segment_stats();

    // let mut td_dijk_server = DijkServer::new(graph.clone());
    // let mut server = Server::new(&cch, &td_cch_graph);

    // let from = Vec::load_from(path.join("uniform_queries/source_node").to_str().unwrap()).expect("could not read source node");
    // let at = Vec::load_from(path.join("uniform_queries/source_time").to_str().unwrap()).expect("could not read source time");
    // let to = Vec::load_from(path.join("uniform_queries/target_node").to_str().unwrap()).expect("could not read target node");

    // let num_queries = 100;

    // let mut dijkstra_time = Duration::zero();
    // let mut tdcch_time = Duration::zero();

    // for ((from, to), at) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).take(num_queries) {
    //     let (ground_truth, time) = measure(|| {
    //         td_dijk_server.distance(from, to, at).map(|dist| dist + at)
    //     });
    //     dijkstra_time =  dijkstra_time + time;

    //     tdcch_time = tdcch_time + measure(|| {
    //         let dist = server.distance(from, to, at).map(|dist| dist + at);
    //         if dist == ground_truth {
    //             println!("TDCCH ✅ {:?} {:?}", dist, ground_truth);
    //         } else {
    //             println!("TDCCH ❌ {:?} {:?}", dist, ground_truth);
    //         }
    //         assert_eq!(dist, ground_truth);
    //     }).1;
    // }
    // println!("Dijkstra {}ms", dijkstra_time.num_milliseconds() / (num_queries as i64));
    // println!("TDCCH {}ms", tdcch_time.num_milliseconds() / (num_queries as i64));
}
