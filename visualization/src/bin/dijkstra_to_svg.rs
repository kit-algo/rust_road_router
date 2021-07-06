// Utility binary to plot the progress of dijkstras algorithm as an SVG file

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::{
        dijkstra::{generic_dijkstra::*, DijkstraData},
        Query,
    },
    cli::CliErr,
    datastr::graph::*,
    io::Load,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f32>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f32>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f32>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f32>()?;

    let start_lat = args.next().ok_or(CliErr("No start_lat arg given"))?.parse::<f32>()?;
    let start_lon = args.next().ok_or(CliErr("No start_lon arg given"))?.parse::<f32>()?;

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let in_bounding_box = |node| lat[node] >= min_lat && lat[node] <= max_lat && lng[node] >= min_lon && lng[node] <= max_lon;

    let graph = FirstOutGraph::new(first_out, head, travel_time);

    println!("<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" viewBox=\"{} {} {} {}\" style=\"transform: scale(1,-1);\" preserveAspectRatio=\"none\">", min_lon, min_lat, max_lon - min_lon, max_lat - min_lat);
    println!("<g>");

    let mut min_dist = std::f32::INFINITY;
    let mut start_node = 0;

    for node in 0..graph.num_nodes() {
        if in_bounding_box(node) {
            let delta_lat = lat[node] - start_lat;
            let delta_lng = lng[node] - start_lon;
            let dist = delta_lat * delta_lat + delta_lng * delta_lng;
            if dist < min_dist {
                min_dist = dist;
                start_node = node;
            }

            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" />",
                lng[node], lat[node], lng[node], lat[node]
            );
            for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
                if in_bounding_box(link.node as usize) {
                    println!(
                        "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" />",
                        lng[node], lat[node], lng[link.node as usize], lat[link.node as usize]
                    );
                }
            }
        }
    }
    println!("</g>");

    let mut ops = DefaultOps();
    let mut data = DijkstraData::<Weight>::new(graph.num_nodes());
    let dijkstra = DijkstraRun::query(
        &graph,
        &mut data,
        &mut ops,
        Query {
            from: start_node as NodeId,
            to: std::u32::MAX,
        },
    );

    let mut counter = 0;
    for node in dijkstra {
        if counter > 500 {
            break;
        }
        counter += 1;
        if in_bounding_box(node as usize) {
            print!("<g class=\"settled fragment\"");
            if counter <= 500 {
                print!(" data-autoslide=\"25\"");
            }
            println!(">");
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" />",
                lng[node as usize], lat[node as usize], lng[node as usize], lat[node as usize]
            );
            for link in LinkIterable::<Link>::link_iter(&graph, node) {
                if in_bounding_box(link.node as usize) {
                    println!(
                        "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" />",
                        lng[node as usize], lat[node as usize], lng[link.node as usize], lat[link.node as usize]
                    );
                }
            }
            println!("</g>");
        }
    }

    println!("</svg>");

    Ok(())
}
