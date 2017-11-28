#[derive(Debug)]
struct TraceData {
    timestamp: u64, // [ms]
    link_id: u64,
    traversed_in_travel_direction_fraction: f32 // [0.0, 1.0]
}

#[derive(Debug)]
struct LinkData {
    link_id: u64,
    length: u32, // [mm]
    speed_limit: u32, // [km/s]
}

#[derive(Debug)]
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

    let mut link_trace_count = 1;
    let mut link_speed_aggregate: Option<f32> = None;

    let mut inbetween_link_count = 0;
    let mut previous_inbetween_link_count = 0;
    let mut inbetween_link_distance = 0;

    while let Some(trace) = traces.next() {
        assert!(trace.timestamp > previous_trace.timestamp);
        let travel_time = trace.timestamp - previous_trace.timestamp;

        if previous_trace.link_id == trace.link_id {
            link_trace_count += 1;

            assert!(trace.traversed_in_travel_direction_fraction > previous_trace.traversed_in_travel_direction_fraction);
            let traveled_fraction = trace.traversed_in_travel_direction_fraction - previous_trace.traversed_in_travel_direction_fraction;
            let traveled_distance = traveled_fraction * current_link.length as f32;
            let speed = traveled_distance / (travel_time as f32); // [mm/ms] = [m/s]

            match link_speed_aggregate {
                Some(ref mut aggregate) => {
                    // linear interpolation
                    *aggregate *= previous_trace.traversed_in_travel_direction_fraction;
                    *aggregate += speed * traveled_distance;
                    *aggregate /= trace.traversed_in_travel_direction_fraction;
                },
                None => {
                    link_speed_aggregate = Some(speed);
                },
            }
        } else {
            loop {
                match lookahead_links.next() {
                    Some(other_link) => {
                        if other_link.link_id == trace.link_id {
                            let current_link_traveled_distance = (current_link.length as f32) * (1.0 - previous_trace.traversed_in_travel_direction_fraction);
                            let other_link_traveled_distance = (other_link.length as f32) * trace.traversed_in_travel_direction_fraction;
                            let total_distance = current_link_traveled_distance + inbetween_link_distance as f32 + other_link_traveled_distance;

                            let speed = total_distance / (travel_time as f32); // [mm/ms] = [m/s]

                            LinkSpeedData {
                                link_id: previous_trace.link_id,
                                link_entered_timestamp: 0, // TODO
                                estimate_quality: 0.0, // TODO
                                velocity: link_speed_aggregate.unwrap() * previous_trace.traversed_in_travel_direction_fraction + speed * (1.0 - previous_trace.traversed_in_travel_direction_fraction)
                            };


                            current_link = other_link;
                            previous_inbetween_link_count = inbetween_link_count;
                            inbetween_link_count = 1; // TODO fucks up counting?
                            inbetween_link_distance = 0;
                            break;
                        } else {
                            inbetween_link_count += 1;
                            inbetween_link_distance += other_link.length;
                        }
                    },
                    None => return Err("Missing link for trace"),
                }
            }
        }

        previous_trace = trace;
    }



    Ok(link_speeds)
}

fn main() {
    let links = vec![];
    let traces = vec![];
    calculate(links.iter(), traces.iter()).expect("no trace points");
}
