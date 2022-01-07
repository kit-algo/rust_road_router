use rust_road_router::{
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        rank_select_map::*,
    },
    io::*,
};
use std::{convert::TryFrom, env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

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
                for EdgeIdT(edge_idx) in graph.edge_indices(from as NodeId, to as NodeId) {
                    let edge_idx = edge_idx as usize;
                    if geo_distance[edge_idx] == 0 {
                        continue;
                    }
                    found += 1;

                    let mut speeds: Vec<u32> = records.map(|s| s.parse::<u32>()).collect::<Result<Vec<_>, _>>()?;

                    let &max_measured = speeds.iter().max().unwrap();
                    // 23:00 - 5:00 there is no traffic! Even if Mapbox believes it.
                    for speed in &mut speeds[23 * 60 / 5..] {
                        *speed = max_measured;
                    }
                    for speed in &mut speeds[..60] {
                        *speed = max_measured;
                    }
                    for speed in &mut speeds {
                        *speed = std::cmp::max(5, *speed);
                    }

                    let speeds_old = speeds.clone();
                    let num_buckets = speeds.len();
                    for i in 0..num_buckets {
                        let idx = |offset: isize| ((i as isize + offset) as usize) % num_buckets;
                        speeds[i] = (speeds_old[idx(-3)]
                            + speeds_old[idx(-2)]
                            + speeds_old[idx(-1)]
                            + speeds_old[i]
                            + speeds_old[idx(1)]
                            + speeds_old[idx(2)]
                            + speeds_old[idx(3)])
                            / 7;
                    }

                    let num_buckets = speeds.len() as Timestamp;
                    let bucket_len = 1000 * 60 * 5;

                    let v_max = geo_distance[edge_idx] * 3600 / graph.weight()[edge_idx];

                    let mut speed_profile: Vec<(Timestamp, u32)> = speeds
                        .iter()
                        .inspect(|&&speed| debug_assert!(speed > 0))
                        .enumerate()
                        .map(|(i, &speed)| (i, std::cmp::min(speed, v_max)))
                        .map(|(i, speed)| (i as Timestamp * bucket_len, speed))
                        .collect();
                    speed_profile.dedup_by_key(|&mut (_, s)| s);
                    speed_profile.push((num_buckets * bucket_len, speed_profile.first().unwrap().1));
                    let profile = speed_profile_to_tt_profile(&speed_profile, geo_distance[edge_idx]);

                    for points in profile.windows(2) {
                        debug_assert!(points[0].0 < points[1].0, "{:#?} {:?}", points, speeds);
                        debug_assert!(points[0].0 + points[0].1 <= points[1].0 + points[1].1, "{:#?} {:?}", points, speeds);
                    }
                    let last = profile.len() - 1;
                    debug_assert!(
                        profile[last].0 + profile[last].1 <= profile[1].0 + profile[1].1 + period(),
                        "{:#?} {:?}",
                        profile,
                        speeds
                    );

                    profile_idx[edge_idx] = Some(profiles.len());
                    profiles.push(profile);

                    break;
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
            ipp_travel_time.push(graph.weight()[arc_idx]);
            first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + 1);
        }
    }

    first_ipp_of_arc.write_to(&path.join("first_ipp_of_arc"))?;
    ipp_departure_time.write_to(&path.join("ipp_departure_time"))?;
    ipp_travel_time.write_to(&path.join("ipp_travel_time"))?;

    Ok(())
}

fn speed_profile_to_tt_profile(speeds: &[(Timestamp, u32)], edge_len: u32) -> Vec<(Timestamp, Weight)> {
    let t_wrap = speeds.last().unwrap().0;
    let last_to_exit = speeds.len() - 2;
    let mut speeds = &*speeds; // reborrow for lifetime foo
    let mut extended_speeds = Vec::new();
    assert!(edge_len > 0);
    assert!(speeds.len() > 1);
    let tt_first = tt(speeds[0].1, edge_len);
    let needs_extension = tt_first > speeds[1].0;
    if needs_extension {
        extended_speeds.extend_from_slice(speeds);
    }

    let mut entered = std::collections::VecDeque::new();
    entered.push_back(0);
    let mut next_to_enter = 1;

    while tt_at_exit(&speeds[*entered.front().unwrap()..=*entered.back().unwrap()], edge_len) > speeds[entered.back().unwrap() + 1].0 {
        let to_add = speeds[next_to_enter];
        extended_speeds.push((to_add.0 + t_wrap, to_add.1));
        entered.push_back(next_to_enter);
        next_to_enter += 1;
    }

    if needs_extension {
        let to_add = speeds[next_to_enter];
        extended_speeds.push((to_add.0 + t_wrap, to_add.1));
        speeds = &extended_speeds;
    }

    let mut profile = Vec::new();
    debug_assert!(tt_first > 0);
    profile.push((0, tt_at_exit(&speeds[*entered.front().unwrap()..=*entered.back().unwrap()], edge_len)));

    while *entered.front().unwrap() <= last_to_exit {
        let next_to_exit = entered.pop_front().unwrap();
        let t_exit = speeds[next_to_exit + 1].0;

        if entered.is_empty() {
            let last_tt = profile.last().unwrap().1;
            let t_enter = t_exit - last_tt;
            if profile.last() != Some(&(t_enter, last_tt)) {
                profile
                    .last()
                    .map(|&(t, _)| debug_assert!(t < t_enter, "{:#?}", (&profile, t_exit, t_enter, last_tt)));
                profile.push((t_enter, last_tt));
            }
            entered.push_back(next_to_enter);
            next_to_enter += 1;
        }

        while entered.back().unwrap() + 1 < speeds.len()
            && tt_at_exit(&speeds[*entered.front().unwrap()..=*entered.back().unwrap()], edge_len) + t_exit > speeds[entered.back().unwrap() + 1].0
        {
            let last = profile.last().unwrap();
            let tt_exit = tt_at_exit(&speeds[*entered.front().unwrap()..=*entered.back().unwrap()], edge_len);
            let delta_at = tt_exit + t_exit - (last.0 + last.1);
            let eval_at = speeds[entered.back().unwrap() + 1].0 - (last.0 + last.1);
            let delta_dt = t_exit - last.0;
            let t_enter = Weight::try_from(u64::from(eval_at) * u64::from(delta_dt) / u64::from(delta_at)).unwrap() + last.0;
            let tt_enter = speeds[entered.back().unwrap() + 1].0 - t_enter;

            if let Some(&(t, tt)) = profile.last() {
                if t >= t_enter {
                    assert_eq!(t, t_enter);
                    assert_eq!(tt, tt_enter);
                    profile.pop();
                }
            }
            profile.push((t_enter, tt_enter));
            entered.push_back(next_to_enter);
            next_to_enter += 1;
        }
        profile.last().map(|&(t, _)| debug_assert!(t < t_exit, "{:#?}", (&profile, t_exit, &speeds)));
        profile.push((t_exit, tt_at_exit(&speeds[*entered.front().unwrap()..=*entered.back().unwrap()], edge_len)))
    }

    debug_assert_eq!(
        profile.last().unwrap().0,
        t_wrap,
        "{:#?}",
        (speeds, &profile, &entered, speeds.len(), needs_extension)
    );
    debug_assert_eq!(
        profile[0].1,
        profile.last().unwrap().1,
        "{:#?}",
        (speeds, &profile, &entered, speeds.len(), needs_extension)
    );
    profile
}

fn tt_at_exit(entered_speeds: &[(Timestamp, u32)], len_m: u32) -> Weight {
    match entered_speeds {
        [(_at, speed)] => tt(*speed, len_m),
        [(at, speed), rest @ ..] => {
            let t_cur = rest[0].0 - at;
            t_cur + tt_at_exit(rest, len_m - speed * t_cur / 3600)
        }
        _ => unreachable!(),
    }
}

fn tt(speed_km_h: u32, len_m: u32) -> Weight {
    100 * 36 * len_m / speed_km_h
}
