use std::collections::LinkedList;
use std::iter::{empty, once};

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

trait State: std::fmt::Debug {
    fn on_link(self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>);
    fn on_trace(self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>);
    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>);
}

#[derive(Debug)]
struct Init {}
impl State for Init {
    fn on_link(self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        (Box::new(InitialLink { link }), Box::new(empty()))
    }

    fn on_trace(self, _trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        panic!("Init: trace before initial link");
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("Init: stream ended, expected link");
    }
}

#[derive(Debug)]
struct InitialLink {
    link: LinkData
}
impl State for InitialLink {
    fn on_link(self, _link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: no trace on initial link");
    }

    fn on_trace(self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        (Box::new(InitialLinkWithTrace { link: self.link, trace }), Box::new(empty()))
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: stream ended, expected trace");
    }
}

#[derive(Debug)]
struct InitialLinkWithTrace {
    link: LinkData,
    trace: TraceData
}
impl State for InitialLinkWithTrace {
    fn on_link(self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_ne!(self.link.link_id, link.link_id);
        let mut intermediates = LinkedList::new();
        intermediates.push_back(link);
        (Box::new(IntermediateLinkAfterInitial { initial_link: self.link, trace: self.trace, intermediates }), Box::new(empty()))
    }

    fn on_trace(self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(trace.link_id, self.trace.link_id);
        assert_eq!(trace.link_id, self.link.link_id);

        let delta_t = trace.timestamp - self.trace.timestamp;
        let delta_fraction = (trace.traversed_in_travel_direction_fraction - self.trace.traversed_in_travel_direction_fraction) as f64;
        let t_pre = (delta_t as f64 * self.trace.traversed_in_travel_direction_fraction as f64 / delta_fraction) as u64;

        debug_assert!(self.trace.timestamp > t_pre, "timestamp underflow");
        let entry_timestamp = self.trace.timestamp - t_pre;

        (Box::new(LinkWithEntryTimestampAndTrace { link: self.link, last_trace: trace, entry_timestamp }), Box::new(empty()))
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLinkWithTrace: stream ended, expected link or trace");
    }
}

#[derive(Debug)]
struct IntermediateLinkAfterInitial {
    initial_link: LinkData,
    trace: TraceData,
    intermediates: LinkedList<LinkData>
}
impl State for IntermediateLinkAfterInitial {
    fn on_link(mut self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push_back(link);
        (Box::new(self), Box::new(empty()))
    }

    fn on_trace(mut self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        let link = self.intermediates.pop_back().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let initial_timestamp = self.trace.timestamp;
        let delta_t = trace.timestamp - initial_timestamp;
        let length_after_initial_trace = ((1.0 - self.trace.traversed_in_travel_direction_fraction as f64) * self.initial_link.length as f64) as u32;
        let length_before_initial_trace = self.initial_link.length - length_after_initial_trace;
        let intermediate_total_length: u32 = self.intermediates.iter().map(|&LinkData { length, .. }| length).sum();
        let length_before_current_trace = (trace.traversed_in_travel_direction_fraction as f64 * link.length as f64) as u32;
        let total_length = length_after_initial_trace + intermediate_total_length + length_before_current_trace;

        let velocity = total_length as f64 / delta_t as f64;
        let time_before_initial = (length_before_initial_trace as f64 / velocity) as u64;
        debug_assert!(time_before_initial < initial_timestamp);
        let link_entered_timestamp = initial_timestamp - time_before_initial;

        let output = once(LinkSpeedData { link_id: self.initial_link.link_id, link_entered_timestamp, estimate_quality: 0.0, velocity: (velocity * 3.6) as f32  });

        let output = output.chain(self.intermediates.into_iter().scan(length_after_initial_trace, move |state, link| {
            *state = *state + link.length;
            let link_entered_timestamp = initial_timestamp + (*state as f64 / velocity) as u64;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality: 0.0, velocity: (velocity * 3.6) as f32 })
        }));

        let entry_timestamp = initial_timestamp + ((length_after_initial_trace + intermediate_total_length) as f64 / velocity) as u64;

        (Box::new(LinkWithEntryTimestampAndTrace { link: link, last_trace: trace, entry_timestamp }), Box::new(output))
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("IntermediateLinkAfterInitial: stream ended, expected link or trace");
    }
}

#[derive(Debug)]
struct LinkWithEntryTimestampAndTrace {
    link: LinkData,
    entry_timestamp: u64,
    last_trace: TraceData,
}
impl State for LinkWithEntryTimestampAndTrace {
    fn on_link(self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        let mut intermediates = LinkedList::new();
        intermediates.push_back(link);
        (Box::new(IntermediateLink { last_link_with_trace: self, intermediates }), Box::new(empty()))
    }

    fn on_trace(mut self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        self.last_trace = trace;
        (Box::new(self), Box::new(empty()))
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        let delta_t = self.last_trace.timestamp - self.entry_timestamp;
        let delta_s = self.last_trace.traversed_in_travel_direction_fraction as f64 * self.link.length as f64;
        let velocity = delta_s / delta_t as f64;
        Box::new(once(LinkSpeedData { link_id: self.link.link_id, link_entered_timestamp: self.entry_timestamp, estimate_quality: 0.0, velocity: (velocity * 3.6) as f32  }))
    }
}

#[derive(Debug)]
struct IntermediateLink {
    last_link_with_trace: LinkWithEntryTimestampAndTrace,
    intermediates: LinkedList<LinkData>
}
impl State for IntermediateLink {
    fn on_link(mut self, link: LinkData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push_back(link);
        (Box::new(self), Box::new(empty()))
    }

    fn on_trace(mut self, trace: TraceData) -> (Box<State>, Box<Iterator<Item = LinkSpeedData>>) {
        let link = self.intermediates.pop_back().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let previous_timestamp = self.last_link_with_trace.last_trace.timestamp;
        let delta_t = trace.timestamp - previous_timestamp;
        let length_after_last_trace = ((1.0 - self.last_link_with_trace.last_trace.traversed_in_travel_direction_fraction as f64) * self.last_link_with_trace.link.length as f64) as u32;
        let intermediate_total_length: u32 = self.intermediates.iter().map(|&LinkData { length, .. }| length).sum();
        let length_before_current_trace = (trace.traversed_in_travel_direction_fraction as f64 * link.length as f64) as u32;
        let total_length = length_after_last_trace + intermediate_total_length + length_before_current_trace;

        let velocity = total_length as f64 / delta_t as f64;

        let time_on_link_after_last_trace = (velocity / length_after_last_trace as f64) as u64;
        let delta_t_link = self.last_link_with_trace.last_trace.timestamp - self.last_link_with_trace.entry_timestamp + time_on_link_after_last_trace;
        let link_velocity = self.last_link_with_trace.link.length as f64 / delta_t_link as f64;

        let output = once(LinkSpeedData { link_id: self.last_link_with_trace.link.link_id, link_entered_timestamp: self.last_link_with_trace.entry_timestamp, estimate_quality: 0.0, velocity: (link_velocity * 3.6) as f32  });

        let output = output.chain(self.intermediates.into_iter().scan(length_after_last_trace, move |state, link| {
            *state = *state + link.length;
            let link_entered_timestamp = previous_timestamp + (*state as f64 / velocity) as u64;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality: 0.0, velocity: (velocity * 3.6) as f32 })
        }));

        let entry_timestamp = previous_timestamp + ((length_after_last_trace + intermediate_total_length) as f64 / velocity) as u64;

        (Box::new(LinkWithEntryTimestampAndTrace { link: link, last_trace: trace, entry_timestamp }), Box::new(output))
    }

    fn on_done(self) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("IntermediateLink: stream ended, expected link or trace");
    }
}


#[derive(Debug)]
struct StateMachine {
    state: Option<Box<State>>
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
