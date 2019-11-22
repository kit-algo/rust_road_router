use bmw_routing_engine::{
    cli::CliErr,
    datastr::{graph::*, rank_select_map::*},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f32>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f32>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f32>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f32>()?;

    let arg = &args.next().ok_or(CliErr("No output directory arg given"))?;
    let out_path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let mut new_first_out = Vec::<u32>::new();
    let mut new_head = Vec::<u32>::new();
    let mut new_travel_time = Vec::new();
    let mut new_lat = Vec::new();
    let mut new_lng = Vec::new();

    new_first_out.push(0);

    let in_bounding_box = |node| lat[node] >= min_lat && lat[node] <= max_lat && lng[node] >= min_lon && lng[node] <= max_lon;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let mut new_nodes = BitVec::new(graph.num_nodes());

    for node in 0..graph.num_nodes() {
        if in_bounding_box(node) {
            new_nodes.set(node);
        }
    }

    let id_map = RankSelectMap::new(new_nodes);

    for node in 0..graph.num_nodes() {
        if in_bounding_box(node) {
            new_first_out.push(*new_first_out.last().unwrap());
            new_lat.push(lat[node]);
            new_lng.push(lng[node]);

            for link in graph.neighbor_iter(node as NodeId) {
                if in_bounding_box(link.node as usize) {
                    *new_first_out.last_mut().unwrap() += 1;
                    new_head.push(id_map.get(link.node as usize).unwrap() as u32);
                    new_travel_time.push(link.weight);
                }
            }
        }
    }

    new_first_out.write_to(&out_path.join("first_out"))?;
    new_head.write_to(&out_path.join("head"))?;
    new_travel_time.write_to(&out_path.join("travel_time"))?;
    new_lat.write_to(&out_path.join("latitude"))?;
    new_lng.write_to(&out_path.join("longitude"))?;

    Ok(())
}
