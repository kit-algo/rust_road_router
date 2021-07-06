#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    cli::CliErr,
    datastr::{graph::*, rank_select_map::*},
    io::*,
    report::*,
};
use std::{env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;
use glob::glob;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_live");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let geo_distance = Vec::<EdgeId>::load_from(path.join("geo_distance"))?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

    for ids in osm_node_ids.windows(2) {
        assert!(ids[0] < ids[1]);
    }

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);

    let arg = &args.next().ok_or(CliErr("No live data directory arg given"))?;
    let live_dir = Path::new(arg);

    rust_road_router::experiments::chpot::run(path, |graph, _rng, live_travel_time| {
        let mut total = 0;
        let mut found = 0;
        let mut too_fast = 0;

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
                total += 1;

                let record = line?;
                let from = record[0].parse()?;
                let to = record[1].parse()?;
                let speed = record[2].parse::<u32>()?;

                if let (Some(from), Some(to)) = (id_map.get(from), id_map.get(to)) {
                    if let Some(edge_idx) = graph.edge_index(from as NodeId, to as NodeId) {
                        found += 1;
                        let edge_idx = edge_idx as usize;

                        let new_tt = 100 * 36 * geo_distance[edge_idx] / speed;
                        if live_travel_time[edge_idx] <= new_tt {
                            live_travel_time[edge_idx] = new_tt;
                        } else {
                            too_fast += 1;
                        }
                    }
                }
            }
        }

        report!("live_traffic", { "num_modifications": total, "num_matched": found, "num_too_fast": too_fast });

        Ok(())
    })
}
