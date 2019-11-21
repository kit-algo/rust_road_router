use std::env;
use std::path::Path;

use bmw_routing_engine::{
    graph::*,
    io::Load,
    shortest_path::{contraction_hierarchy, node_order::*},
};

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let min_lat = args
        .next()
        .map(|s| s.parse::<f32>().unwrap_or_else(|_| panic!("could not parse {} as lat coord", s)))
        .unwrap();
    let min_lon = args
        .next()
        .map(|s| s.parse::<f32>().unwrap_or_else(|_| panic!("could not parse {} as lon coord", s)))
        .unwrap();
    let max_lat = args
        .next()
        .map(|s| s.parse::<f32>().unwrap_or_else(|_| panic!("could not parse {} as lat coord", s)))
        .unwrap();
    let max_lon = args
        .next()
        .map(|s| s.parse::<f32>().unwrap_or_else(|_| panic!("could not parse {} as lon coord", s)))
        .unwrap();

    let contraction_count = args
        .next()
        .map(|s| {
            s.parse::<usize>()
                .unwrap_or_else(|_| panic!("could not parse {} as number of contracted nodes", s))
        })
        .unwrap();

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    let order = Vec::load_from(path.join("ch_order").to_str().unwrap()).expect("could not read ch_order");
    let node_order = NodeOrder::from_node_order(order.clone());
    let lat = Vec::<f32>::load_from(path.join("latitude").to_str().unwrap()).expect("could not read latitude");
    let lng = Vec::<f32>::load_from(path.join("longitude").to_str().unwrap()).expect("could not read longitude");

    let in_bounding_box = |node| lat[node] >= min_lat && lat[node] <= max_lat && lng[node] >= min_lon && lng[node] <= max_lon;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let (up, down) = contraction_hierarchy::overlay(&graph, order, contraction_count);

    println!("<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" viewBox=\"{} {} {} {}\" style=\"transform: scale(1,-1);\" preserveAspectRatio=\"none\">", min_lon, min_lat, max_lon - min_lon, max_lat - min_lat);
    println!("<g>");

    for &node in &node_order.order()[contraction_count..] {
        let node = node as usize;
        if in_bounding_box(node) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" />",
                lng[node], lat[node], lng[node], lat[node]
            );

            for link in up.neighbor_iter(node_order.rank(node as NodeId)) {
                let link_node = node_order.node(link.node) as usize;
                if in_bounding_box(link_node) {
                    println!(
                        "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" />",
                        lng[node], lat[node], lng[link_node], lat[link_node]
                    );
                }
            }

            for link in down.neighbor_iter(node_order.rank(node as NodeId)) {
                let link_node = node_order.node(link.node) as usize;
                if in_bounding_box(link_node) {
                    println!(
                        "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" />",
                        lng[link_node], lat[link_node], lng[node], lat[node]
                    );
                }
            }
        }
    }

    println!("</g>");
    println!("</svg>");
}
