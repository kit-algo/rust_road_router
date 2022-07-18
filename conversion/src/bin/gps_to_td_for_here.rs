#![feature(array_windows)]

use conversion::here::link_id_mapper::*;
use conversion::speed_profile_to_tt_profile;
use rust_road_router::{
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        rank_select_map::*,
    },
    io::*,
};
use std::{collections::BTreeMap, env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;
use flate2::read::GzDecoder;

const NUM_BUCKETS: usize = 24 * 4;
const MIN_NUM_SAMPELS: usize = 3;
const TOO_FAST_FACTOR: f64 = 1.5;
const BUCKET_LEN: u32 = period() / NUM_BUCKETS as u32;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);
    let start_time: u64 = args.next().expect("No start time given").parse().expect("could not parse start time");
    let end_time: u64 = args.next().expect("No end time given").parse().expect("could not parse end time");

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;

    let link_id_mapping = BitVec::load_from(path.join("link_id_mapping"))?;
    let link_id_mapping = InvertableRankSelectMap::new(RankSelectMap::new(link_id_mapping));
    let here_rank_to_link_id = Vec::load_from(path.join("here_rank_to_link_id"))?;
    let id_mapper = LinkIdMapper::new(link_id_mapping, here_rank_to_link_id, graph.num_arcs());

    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0];
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0];

    let speeds_kmph: Box<[_]> = graph
        .weight()
        .iter()
        .zip(geo_distance.iter())
        .map(|(tt, d)| (d * tt_units_per_s * 36) / (tt * dist_units_per_m * 10))
        .collect();

    let mut speed_profiles = vec![BTreeMap::<usize, Vec<(Timestamp, f64)>>::new(); graph.num_arcs()];

    let mut total = 0;
    let mut applied = 0;
    let mut too_fast = 0;
    let mut no_edge = 0;
    let mut out_of_range = 0;
    let mut invalid = 0;

    for file in args {
        let file = File::open(file.clone()).unwrap();
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .quoting(false)
            .double_quote(false)
            .escape(None)
            .from_reader(GzDecoder::new(file));

        let mut timestamp_column_idx = None;
        let mut link_id_column_idx = None;
        let mut dir_column_idx = None;
        let mut speed_column_idx = None;

        for (idx, header) in reader.headers().expect("csv missing headers").iter().enumerate() {
            match header {
                "mm_linkId" => link_id_column_idx = Some(idx),
                "mm_isFromRef" => dir_column_idx = Some(idx),
                "mm_speed_mps" => speed_column_idx = Some(idx),
                "timestampUTC_ms" => timestamp_column_idx = Some(idx),
                _ => (),
            }
        }

        let timestamp_column_idx = timestamp_column_idx.unwrap();
        let link_id_column_idx = link_id_column_idx.unwrap();
        let dir_column_idx = dir_column_idx.unwrap();
        let speed_column_idx = speed_column_idx.unwrap();

        for line in reader.records() {
            total += 1;

            let mut timestamp: u64 = 0;
            let mut link_id: u64 = 0;
            let mut dir: LinkDirection = LinkDirection::FromRef;
            let mut speed_kmph: f64 = 1000.0;

            for (idx, record) in line.unwrap().iter().enumerate() {
                if idx == timestamp_column_idx {
                    timestamp = record.parse().unwrap();
                } else if idx == link_id_column_idx {
                    link_id = record.parse().unwrap();
                } else if idx == dir_column_idx {
                    match record {
                        "true" => dir = LinkDirection::FromRef,
                        "false" => dir = LinkDirection::ToRef,
                        _ => panic!("broken direction"),
                    }
                } else if idx == speed_column_idx {
                    if let Ok(speed) = record.parse::<f64>() {
                        speed_kmph = speed * 3.6;
                    } else {
                        invalid += 1;
                    }
                }
            }

            if let Some(edge_idx) = id_mapper.here_to_local_link_id(link_id, dir) {
                // check if speed too large
                if speeds_kmph[edge_idx as usize] as f64 * TOO_FAST_FACTOR < speed_kmph {
                    too_fast += 1;
                    continue;
                }
                if timestamp < start_time || timestamp >= end_time {
                    out_of_range += 1;
                    continue;
                }
                let t = (timestamp - start_time) as Timestamp % period();
                // timestamp to bucket
                let bucket = ((timestamp - start_time) / BUCKET_LEN as u64) as usize % NUM_BUCKETS;
                let entry = speed_profiles[edge_idx as usize].entry(bucket).or_insert(Vec::new());
                entry.push((t, speed_kmph));
                applied += 1;
            } else {
                no_edge += 1;
            }
        }
    }

    dbg!(total, applied, too_fast, no_edge, out_of_range, invalid);

    let mut first_ipp_of_arc = Vec::with_capacity(graph.num_arcs() + 1);
    first_ipp_of_arc.push(0u32);
    let mut ipp_departure_time = Vec::<Timestamp>::new();
    let mut ipp_travel_time = Vec::<Weight>::new();

    for (edge_idx, speed_profile) in speed_profiles.iter_mut().enumerate() {
        let mut constant = true;
        if !speed_profile.is_empty() {
            let mut flat_speed_profile = vec![speeds_kmph[edge_idx]; NUM_BUCKETS];
            for (&bucket_id, speeds) in speed_profile.iter_mut() {
                if speeds.len() >= MIN_NUM_SAMPELS {
                    speeds.sort_unstable_by_key(|&(t, _)| t);
                    let mut speed_sum = 0.0;
                    let mut weight_sum = 0.0;

                    let weight = (speeds[1].0 - (bucket_id as u32 * BUCKET_LEN)) as f64;
                    weight_sum += weight;
                    speed_sum += speeds[0].1 * weight;
                    for [(t_prev, _), (_, speed), (t_next, _)] in speeds.array_windows() {
                        let weight = (t_next - t_prev) as f64;
                        weight_sum += weight;
                        speed_sum += speed * weight;
                    }
                    let weight = (((bucket_id + 1) as u32 * BUCKET_LEN) - speeds[speeds.len() - 2].0) as f64;
                    weight_sum += weight;
                    speed_sum += speeds[speeds.len() - 1].1 * weight;

                    flat_speed_profile[bucket_id] = std::cmp::min(speeds_kmph[edge_idx], std::cmp::max(1, (speed_sum / weight_sum).round() as u32));
                }
            }

            let mut linear_speed_profile = flat_speed_profile.iter().enumerate().fold(Vec::new(), |mut acc, (idx, &speed)| {
                if let Some(&(_, last_speed)) = acc.last() {
                    if speed != last_speed {
                        acc.push((idx as u32 * BUCKET_LEN, speed));
                    }
                } else {
                    debug_assert_eq!(idx, 0);
                    acc.push((idx as u32 * BUCKET_LEN, speed));
                }
                acc
            });
            if linear_speed_profile.len() > 1 {
                linear_speed_profile.push((period(), linear_speed_profile[0].1));

                let tt_profile = speed_profile_to_tt_profile(&linear_speed_profile, geo_distance[edge_idx] / dist_units_per_m);

                for points in tt_profile.windows(2) {
                    debug_assert!(points[0].0 < points[1].0, "{:#?} {:?}", points, linear_speed_profile);
                    debug_assert!(
                        points[0].0 + points[0].1 <= points[1].0 + points[1].1,
                        "{:#?} {:?}",
                        points,
                        linear_speed_profile
                    );
                }
                let last = tt_profile.len() - 1;
                debug_assert!(
                    tt_profile[last].0 + tt_profile[last].1 <= tt_profile[1].0 + tt_profile[1].1 + period(),
                    "{:#?} {:?}",
                    tt_profile,
                    linear_speed_profile
                );

                constant = false;
                for &(dt, tt) in &tt_profile {
                    ipp_departure_time.push(dt);
                    ipp_travel_time.push(tt);
                }
                first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + tt_profile.len() as u32);
            }
        }
        if constant {
            ipp_departure_time.push(0);
            ipp_travel_time.push(graph.weight()[edge_idx]);
            first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + 1);
        }
    }

    first_ipp_of_arc.write_to(&path.join("first_ipp_of_arc"))?;
    ipp_departure_time.write_to(&path.join("ipp_departure_time"))?;
    ipp_travel_time.write_to(&path.join("ipp_travel_time"))?;

    Ok(())
}
