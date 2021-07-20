// Utility to check OSM Live data and determine how many of the referenced nodes are known.

use rust_road_router::{
    cli::CliErr,
    datastr::{graph::*, rank_select_map::*},
    io::*,
};
use std::{env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;
use glob::glob;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

    for ids in osm_node_ids.windows(2) {
        assert!(ids[0] < ids[1]);
    }

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);
    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;

    let arg = &args.next().ok_or(CliErr("No live data directory arg given"))?;
    let live_dir = Path::new(arg);

    let mut total_count = 0;
    let mut nodes_found_count = 0;
    let mut arc_found_count = 0;

    for live in glob(live_dir.join("*").to_str().unwrap()).unwrap() {
        let file = File::open(live?.clone()).unwrap();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .quoting(false)
            .double_quote(false)
            .escape(None)
            .from_reader(file);

        for line in reader.records() {
            total_count += 1;

            let record = line?;
            let from = record[0].parse()?;
            let to = record[1].parse()?;

            if let (Some(from), Some(to)) = (id_map.get(from), id_map.get(to)) {
                nodes_found_count += 1;

                if graph.edge_indices(from as NodeId, to as NodeId).next().is_some() {
                    arc_found_count += 1;
                }
            }
        }
    }

    eprintln!(
        "known nodes: {}% ({}/{})",
        nodes_found_count * 100 / total_count,
        nodes_found_count,
        total_count
    );
    eprintln!("known arcs: {}% ({}/{})", arc_found_count * 100 / total_count, arc_found_count, total_count);

    Ok(())
}
