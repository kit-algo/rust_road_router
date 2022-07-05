use rust_road_router::{cli::CliErr, datastr::graph::*, io::*};

use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);
    let arg = &args.next().ok_or(CliErr("No output directory arg given"))?;
    let out_path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;
    let tt_units_per_second = Vec::<Weight>::load_from(path.join("tt_units_per_second"))?[0];

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    let exp_graph = line_graph(&graph, |edge1_idx, edge2_idx| {
        if tail[edge1_idx as usize] == graph.head()[edge2_idx as usize] {
            return Some(100 * tt_units_per_second);
        }
        Some(0)
    });

    let new_lat: Vec<_> = (0..exp_graph.num_nodes()).map(|idx| lat[tail[idx] as usize]).collect();
    let new_lng: Vec<_> = (0..exp_graph.num_nodes()).map(|idx| lng[tail[idx] as usize]).collect();

    exp_graph.first_out().write_to(&out_path.join("first_out"))?;
    exp_graph.head().write_to(&out_path.join("head"))?;
    exp_graph.weight().write_to(&out_path.join("travel_time"))?;
    new_lat.write_to(&out_path.join("latitude"))?;
    new_lng.write_to(&out_path.join("longitude"))?;
    [tt_units_per_second].write_to(&out_path.join("tt_units_per_second"))?;

    Ok(())
}
