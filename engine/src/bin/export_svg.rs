// Plot a subgraph within given geographic boundaries to a SVG.

use rust_road_router::{
    algo::{dijkstra::generic_dijkstra::*, Query},
    cli::CliErr,
    datastr::graph::*,
    io::*,
    util::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f64>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f64>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f64>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f64>()?;

    let max_x = args.next().ok_or(CliErr("No max_x arg given"))?.parse::<f64>()?;
    let max_y = args.next().ok_or(CliErr("No max_y arg given"))?.parse::<f64>()?;

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let graph = FirstOutGraph::new(first_out, head, travel_time);

    let x_coord = |node| (lng[node] as f64 - min_lon) * max_x / (max_lon - min_lon);
    let y_coord = |node| (max_lat - lat[node] as f64) * max_y / (max_lat - min_lat);

    let from = (0..graph.num_nodes())
        .min_by_key(|&n| {
            NonNan::new((lat[n] - 49.00815772031336) * (lat[n] - 49.00815772031336) + (lng[n] - 8.403795863852542) * (lng[n] - 8.403795863852542)).unwrap()
        })
        .unwrap() as NodeId;

    let mut dijkstra_rank = vec![0; graph.num_nodes()];
    let mut forward_dijkstra = GenericDijkstra::<OwnedGraph, DefaultOps, &OwnedGraph>::new(&graph);
    forward_dijkstra.initialize_query(Query { from, to: std::u32::MAX });
    for (i, node) in forward_dijkstra.enumerate() {
        dijkstra_rank[node as usize] = i;
    }

    println!(
        "<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" viewBox=\"0 0 {} {}\">",
        max_x, max_y
    );
    println!("<g>");

    for node in 0..graph.num_nodes() {
        println!(
            "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--settled: {};\" />",
            x_coord(node),
            y_coord(node),
            x_coord(node),
            y_coord(node),
            dijkstra_rank[node]
        );
        for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" />",
                x_coord(node),
                y_coord(node),
                x_coord(link.node as usize),
                y_coord(link.node as usize),
            );
        }
    }

    println!("</g>");
    println!("</svg>");

    Ok(())
}
