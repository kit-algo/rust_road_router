// Plot a subgraph within given geographic boundaries to a SVG.

use bmw_routing_engine::{cli::CliErr, datastr::graph::*, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f32>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f32>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f32>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f32>()?;

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let in_bounding_box = |node| lat[node] >= min_lat && lat[node] <= max_lat && lng[node] >= min_lon && lng[node] <= max_lon;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    println!("<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" viewBox=\"{} {} {} {}\" style=\"transform: scale(1,-1);\" preserveAspectRatio=\"none\">", min_lon, min_lat, max_lon - min_lon, max_lat - min_lat);
    println!("<g>");

    for node in 0..graph.num_nodes() {
        if in_bounding_box(node) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" />",
                lng[node], lat[node], lng[node], lat[node]
            );
            for link in graph.neighbor_iter(node as NodeId) {
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
    println!("</svg>");

    Ok(())
}
