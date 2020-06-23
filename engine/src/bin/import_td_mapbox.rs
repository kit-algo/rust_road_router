use bmw_routing_engine::{
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        rank_select_map::*,
    },
    io::*,
};
use std::{env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let travel_time = Vec::<Weight>::load_from(path.join("travel_time"))?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    for ids in osm_node_ids.windows(2) {
        assert!(ids[0] < ids[1]);
    }

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);

    let mut profile_idx = vec![None; graph.num_arcs()];
    let mut profiles = Vec::new();

    let mut total = 0;
    let mut found = 0;

    for file in args {
        let file = File::open(file.clone()).unwrap();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .quoting(false)
            .double_quote(false)
            .escape(None)
            .from_reader(file);

        for line in reader.records() {
            total += 1;

            let records = line?;
            let mut records = records.iter();
            let from = records.next().unwrap().parse()?;
            let to = records.next().unwrap().parse()?;

            if let (Some(from), Some(to)) = (id_map.get(from), id_map.get(to)) {
                if let Some(edge_idx) = graph.edge_index(from as NodeId, to as NodeId) {
                    found += 1;
                    let edge_idx = edge_idx as usize;

                    // TODO by day
                    let speeds: Vec<u32> = records
                        .skip(60 / 5 * 24 * 2)
                        .take(60 / 5 * 24)
                        .map(|s| s.parse::<u32>())
                        .collect::<Result<Vec<_>, _>>()?;

                    // let buckets = speeds.len();
                    // for i in 0..buckets {
                    //     use std::cmp::min;
                    //     let next = speeds[(i + 1) % buckets];
                    //     let prev = speeds[(i + buckets - 1) % buckets];
                    //     let min_around = min(next, prev);
                    //     let cur = &mut speeds[i];
                    //     if *cur * 2 < min_around {
                    //         *cur = min_around;
                    //     }
                    // }

                    let num_buckets = speeds.len() as Timestamp;

                    let mut tts: Vec<(Timestamp, Weight)> = speeds
                        .iter()
                        .enumerate()
                        .map(|(i, speed)| (i as Timestamp * 1000 * 60 * 5, 100 * 36 * geo_distance[edge_idx] / speed))
                        .collect();
                    tts.dedup_by_key(|&mut (_, tt)| tt);
                    tts.push((num_buckets * 1000 * 60 * 5, tts.first().unwrap().1));
                    let mut profile: Vec<_> = tts
                        .windows(2)
                        .flat_map(|points| {
                            if points[1].0 > points[0].1 && points[1].0 - points[0].1 > points[0].0 {
                                vec![points[0], (points[1].0 - points[0].1, points[0].1)]
                            } else {
                                vec![points[0]]
                            }
                            .into_iter()
                        })
                        .collect();

                    for _ in 0..2 {
                        for i in (0..profile.len()).rev() {
                            profile[i].1 = std::cmp::min(
                                profile[i].1,
                                profile[(i + 1) % profile.len()].1 + (profile[(i + 1) % profile.len()].0 + 1000 * 3600 * 24 - profile[i].0) % 1000 * 3600 * 24,
                            );
                        }
                    }

                    profile_idx[edge_idx] = Some(profiles.len());
                    profiles.push(profile);
                }
            }
        }
    }

    dbg!(total, found);

    let mut first_ipp_of_arc = Vec::with_capacity(graph.num_arcs() + 1);
    first_ipp_of_arc.push(0u32);
    let mut ipp_departure_time = Vec::<Timestamp>::new();
    let mut ipp_travel_time = Vec::<Weight>::new();

    for (arc_idx, &idx) in profile_idx.iter().enumerate() {
        if let Some(idx) = idx {
            for &(dt, tt) in &profiles[idx] {
                ipp_departure_time.push(dt);
                ipp_travel_time.push(tt);
            }
            first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + profiles[idx].len() as u32);
        } else {
            ipp_departure_time.push(0);
            ipp_travel_time.push(travel_time[arc_idx]);
            first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + 1);
        }
    }

    first_ipp_of_arc.write_to(&path.join("first_ipp_of_arc"))?;
    ipp_departure_time.write_to(&path.join("ipp_departure_time"))?;
    ipp_travel_time.write_to(&path.join("ipp_travel_time"))?;

    Ok(())
}
