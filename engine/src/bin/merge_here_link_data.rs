// Program to convert map data from HERE into a csv with a list of all relevant links and necessary info about each one.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    cli::CliErr,
    datastr::{graph::*, rank_select_map::*},
    import::here::{csv_source::CSVSource, link_id_mapper::*, read_graph},
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let in_dir = &args.next().ok_or(CliErr("No input directory arg given"))?;

    let source = CSVSource::new(Path::new(in_dir));
    let data = read_graph(&source, (-360 * 100_000, -360 * 100_000), (360 * 100_000, 360 * 100_000));

    let graph = data.graph;
    let link_id_mapper = LinkIdMapper::new(
        InvertableRankSelectMap::new(data.link_id_mapping),
        data.here_rank_to_link_id,
        graph.head().len(),
    );

    println!("tail,head,mm_linkLength_m,linkId,direction");

    let mut edge_id = 0;
    for node in 0..graph.num_nodes() {
        for link in graph.neighbor_iter(node as NodeId) {
            let (here_link_id, direction) = link_id_mapper.local_to_here_link_id(edge_id);
            let direction = match direction {
                LinkDirection::FromRef => 'F',
                LinkDirection::ToRef => 'T',
            };
            println!("{},{},{},{},{}", node, link.node, data.link_lengths[edge_id as usize], here_link_id, direction);
            edge_id += 1;
        }
    }

    Ok(())
}
