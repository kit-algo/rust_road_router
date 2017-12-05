#[derive(Debug)]
struct TraceData {
    timestamp: u64, // [ms]
    link_id: u64,
    traversed_in_travel_direction_fraction: f32 // [0.0, 1.0]
}

#[derive(Debug, PartialEq)]
struct LinkData {
    link_id: u64,
    length: u32, // [mm]
    speed_limit: u32, // [km/s]
}

#[derive(Debug, PartialEq)]
struct LinkSpeedData {
    link_id: u64,
    link_entered_timestamp: u64,
    estimate_quality: f32,
    velocity: f32
}

fn calculate<'a, LinkIterator: Iterator<Item = &'a LinkData> + Clone, TraceIterator: Iterator<Item = &'a TraceData>>(mut links: LinkIterator, mut traces: TraceIterator) -> Result<Vec<LinkSpeedData>, &'static str> {
    let mut link_speeds = Vec::with_capacity(links.size_hint().0);
    let mut lookahead_links = links.clone();

    let mut previous_trace = traces.next().ok_or("Empty trace iterator")?;
    let mut current_link = lookahead_links.next().ok_or("Empty link iterator")?;
    assert!(current_link.link_id == previous_trace.link_id);

    while let Some(current_trace) = traces.next() {
        debug_assert!(current_trace.timestamp > previous_trace.timestamp);
        let travel_time = current_trace.timestamp - previous_trace.timestamp;

        if previous_trace.link_id == current_trace.link_id {
            let traveled_fraction = (current_trace.traversed_in_travel_direction_fraction - previous_trace.traversed_in_travel_direction_fraction) as f64;
            let traveled_distance = traveled_fraction * current_link.length as f64;
            let speed = traveled_distance / (travel_time as f64); // [mm/ms] = [m/s]
            let link_entered_timestamp = previous_trace.timestamp - (previous_trace.traversed_in_travel_direction_fraction as f64 * current_link.length as f64 / speed) as u64;
            link_speeds.push(LinkSpeedData { link_id: current_link.link_id, link_entered_timestamp, estimate_quality: 0.0, velocity: (speed * 3.6) as f32 });
    //         link_trace_count += 1;

    //         assert!(trace.traversed_in_travel_direction_fraction > previous_trace.traversed_in_travel_direction_fraction);
    //         let traveled_fraction = trace.traversed_in_travel_direction_fraction - previous_trace.traversed_in_travel_direction_fraction;
    //         let traveled_distance = traveled_fraction * current_link.length as f32;
    //         let speed = traveled_distance / (travel_time as f32); // [mm/ms] = [m/s]

    //         match link_speed_aggregate {
    //             Some(ref mut aggregate) => {
    //                 // linear interpolation
    //                 *aggregate *= previous_trace.traversed_in_travel_direction_fraction;
    //                 *aggregate += speed * traveled_distance;
    //                 *aggregate /= trace.traversed_in_travel_direction_fraction;
    //             },
    //             None => {
    //                 link_speed_aggregate = Some(speed);
    //             },
    //         }
        } else {
            let traveled_distance_prev = (1.0 - previous_trace.traversed_in_travel_direction_fraction as f64) * current_link.length as f64;
            let mut traveled_distance = traveled_distance_prev;

            loop {
                match lookahead_links.next() {
                    Some(next_link) if next_link.link_id == current_trace.link_id => {
                        let traveled_distance_cur = (1.0 - current_trace.traversed_in_travel_direction_fraction as f64) * next_link.length as f64;
                        traveled_distance += traveled_distance_cur;
                        let speed = traveled_distance / (travel_time as f64); // [mm/ms] = [m/s]

                        let mut next_ts = previous_trace.timestamp + ((1.0 - previous_trace.traversed_in_travel_direction_fraction as f64) * current_link.length as f64 / speed) as u64;

                        loop {
                            match links.next() {
                                // link with first trace point case
                                Some(link_to_emit) if link_to_emit == current_link => {
                                    let link_entered_timestamp = previous_trace.timestamp - (previous_trace.traversed_in_travel_direction_fraction as f64 * link_to_emit.length as f64 / speed) as u64;
                                    link_speeds.push(LinkSpeedData { link_id: link_to_emit.link_id, link_entered_timestamp, estimate_quality: 0.0, velocity: (speed * 3.6) as f32 });
                                },
                                // link with the current trace point
                                Some(link_to_emit) if link_to_emit == next_link => {
                                    link_speeds.push(LinkSpeedData { link_id: link_to_emit.link_id, link_entered_timestamp: next_ts, estimate_quality: 0.0, velocity: (speed * 3.6) as f32 });
                                    break;
                                    // todo, what happens to velocity when more tracepoints are available for link?
                                },
                                // inbetween links
                                Some(link_to_emit) => {
                                    link_speeds.push(LinkSpeedData { link_id: link_to_emit.link_id, link_entered_timestamp: next_ts, estimate_quality: 0.0, velocity: (speed * 3.6) as f32 });
                                    next_ts += (link_to_emit.length as f64 / speed) as u64;
                                },
                                None => panic!("should be unreachable"),
                            }
                        }

                        current_link = next_link;
                        break;
                    },
                    Some(next_link) => {
                        traveled_distance += next_link.length as f64;
                    },
                    _ => return Err("Missing link for trace"),
                }
            }
        }

        previous_trace = current_trace;
    }



    Ok(link_speeds)
}

fn main() {
    let links = vec![];
    let traces = vec![];
    calculate(links.iter(), traces.iter()).expect("no trace points");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_for_empty_errors() {
        let links = vec![];
        let traces = vec![];
        assert!(calculate(links.iter(), traces.iter()).is_err());
    }

    #[test]
    fn two_points_one_link() {
        let links = vec![LinkData { link_id: 1, length: 10000, speed_limit: 50 }];
        let traces = vec![
            TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.1 },
            TraceData { timestamp: 101000, link_id: 1, traversed_in_travel_direction_fraction: 0.9 }
        ];
        assert_eq!(calculate(links.iter(), traces.iter()).unwrap(),
            vec![LinkSpeedData { link_id: 1, link_entered_timestamp: 99875, estimate_quality: 0.0, velocity: 28.8 }]);
    }

    #[test]
    fn two_points_two_links() {
        let links = vec![
            LinkData { link_id: 1, length: 10000, speed_limit: 80 },
            LinkData { link_id: 2, length: 30000, speed_limit: 80 }
        ];
        let traces = vec![
            TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.5 },
            TraceData { timestamp: 101000, link_id: 2, traversed_in_travel_direction_fraction: 0.5 }
        ];
        assert_eq!(calculate(links.iter(), traces.iter()).unwrap(),
            vec![
                LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.0, velocity: 72.0 },
                LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 0.0, velocity: 72.0 }
            ]);
    }

    #[test]
    fn two_points_three_links() {
        let links = vec![
            LinkData { link_id: 1, length: 10000, speed_limit: 80 },
            LinkData { link_id: 2, length: 30000, speed_limit: 80 },
            LinkData { link_id: 3, length: 10000, speed_limit: 80 },
        ];
        let traces = vec![
            TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.5 },
            TraceData { timestamp: 102000, link_id: 3, traversed_in_travel_direction_fraction: 0.5 }
        ];
        assert_eq!(calculate(links.iter(), traces.iter()).unwrap(),
            vec![
                LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.0, velocity: 72.0 },
                LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 0.0, velocity: 72.0 },
                LinkSpeedData { link_id: 3, link_entered_timestamp: 101750, estimate_quality: 0.0, velocity: 72.0 },
            ]);
    }
}
