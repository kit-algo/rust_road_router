use std::{env, error::Error, path::Path};

use bmw_routing_engine::{
    cli::CliErr,
    graph::*,
    io::Load,
    shortest_path::{contraction_hierarchy, node_order::*},
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f32>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f32>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f32>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f32>()?;

    let contraction_count = args.next().ok_or(CliErr("No contraction count arg given"))?.parse::<usize>()?;

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap())?;
    let head = Vec::load_from(path.join("head").to_str().unwrap())?;
    let travel_time = Vec::load_from(path.join("travel_time").to_str().unwrap())?;
    let order = Vec::load_from(path.join("ch_order").to_str().unwrap())?;
    let node_order = NodeOrder::from_node_order(order.clone());
    let lat = Vec::<f32>::load_from(path.join("latitude").to_str().unwrap())?;
    let lng = Vec::<f32>::load_from(path.join("longitude").to_str().unwrap())?;

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

    Ok(())
}
