use std::{env, path::Path};

use bmw_routing_engine::{
    benchmark::*,
    graph::time_dependent::*,
    io::*,
    shortest_path::{
        customizable_contraction_hierarchy::*,
        node_order::NodeOrder,
        query::{td_astar::Server, td_dijkstra::Server as DijkServer},
    },
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
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    eprintln!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), ipp_departure_time.len());

    let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
    let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

    let mut added = 0;

    for i in 0..head.len() {
        let range = first_ipp_of_arc[i] as usize..first_ipp_of_arc[i + 1] as usize;
        assert_ne!(range.start, range.end);

        first_ipp_of_arc[i] += added;

        if range.end - range.start > 1 {
            if ipp_departure_time[range.start] != 0 {
                new_ipp_departure_time.push(0);
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
                added += 1;
            }
            new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
            new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
            new_ipp_departure_time.push(period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 1;
        } else {
            new_ipp_departure_time.push(0);
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
        }
    }
    first_ipp_of_arc[head.len()] += added;

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, new_ipp_departure_time, new_ipp_travel_time);
    let cch_order = Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm");
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order.clone());
    let cch_order = CCHReordering {
        node_order: cch_order,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization(cch.separators());
    let cch = contract(&graph, cch_order);

    let mut td_dijk_server = DijkServer::new(graph.clone());
    let mut server = Server::new(graph, &cch);

    let mut query_dir = None;
    let mut base_dir = Some(path);

    while let Some(base) = base_dir {
        if base.join("uniform_queries").exists() {
            query_dir = Some(base.join("uniform_queries"));
            break;
        } else {
            base_dir = base.parent();
        }
    }

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node").to_str().unwrap()).expect("could not read source node");
        let at = Vec::<u32>::load_from(path.join("source_time").to_str().unwrap()).expect("could not read source time");
        let to = Vec::load_from(path.join("target_node").to_str().unwrap()).expect("could not read target node");

        let num_queries = 50;

        let mut dijkstra_time = Duration::zero();
        let mut astar_time = Duration::zero();

        for ((from, to), at) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).take(num_queries) {
            let (ground_truth, time) = measure(|| td_dijk_server.distance(from, to, at).map(|dist| dist + at));

            dijkstra_time = dijkstra_time + time;

            let (ea, time) = measure(|| server.distance(from, to, at).map(|dist| dist + at));

            astar_time = astar_time + time;

            assert_eq!(ground_truth, ea);
        }
        eprintln!("Dijkstra {}", dijkstra_time / (num_queries as i32));
        eprintln!("A* {}", astar_time / (num_queries as i32));
    }
}
