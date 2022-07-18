use rust_road_router::datastr::graph::{time_dependent::*, *};

pub mod here;

pub fn speed_profile_to_tt_profile(speeds: &[(Timestamp, u32)], edge_len: u32) -> Vec<(Timestamp, Weight)> {
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
    // if speed_km_h == 0 {
    //     return INFINITY;
    // }
    100 * 36 * len_m / speed_km_h
}
