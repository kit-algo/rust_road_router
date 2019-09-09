use bmw_routing_engine::{
    graph::*,
    io::*,
    cli::CliErr,
    rank_select_map::*,
};
use std::{env, error::Error, path::Path, fs::File};

use csv::ReaderBuilder;
use glob::glob;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or_else(|| Box::new(CliErr("No graph directory arg given")))?;
    let path = Path::new(arg);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out").to_str().unwrap())?;
    let head = Vec::<EdgeId>::load_from(path.join("head").to_str().unwrap())?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids").to_str().unwrap())?;

    for ids in osm_node_ids.windows(2) {
        assert!(ids[0] < ids[1]);
    }

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &head[..]);

    let arg = &args.next().ok_or_else(|| Box::new(CliErr("No live data directory arg given")))?;
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

                if graph.edge_index(from as NodeId, to as NodeId).is_some() {
                    arc_found_count += 1;
                }
            }
        }
    }

    eprintln!("known nodes: {}% ({}/{})", nodes_found_count * 100 / total_count, nodes_found_count, total_count);
    eprintln!("known arcs: {}% ({}/{})", arc_found_count * 100 / total_count, arc_found_count, total_count);

    Ok(())
}
