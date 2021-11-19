use rust_road_router::{cli::CliErr, datastr::graph::*, io::*};
use std::{env, error::Error, path::Path};

use nav_types::WGS84;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;
    let latitude = Vec::<f32>::load_from(path.join("latitude"))?;
    let longitude = Vec::<f32>::load_from(path.join("longitude"))?;
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0];

    let mut geo_distances = Vec::<Weight>::with_capacity(graph.num_arcs());

    for node in 0..graph.num_nodes() {
        for NodeIdT(head) in LinkIterable::<NodeIdT>::link_iter(&graph, node as NodeId) {
            geo_distances.push(
                (WGS84::from_degrees_and_meters(latitude[node] as f64, longitude[node] as f64, 0.0).distance(&WGS84::from_degrees_and_meters(
                    latitude[head as usize] as f64,
                    longitude[head as usize] as f64,
                    0.0,
                )) * dist_units_per_m as f64) as Weight,
            );
        }
    }

    Ok(geo_distances.write_to(&path.join("geo_distance"))?)
}
