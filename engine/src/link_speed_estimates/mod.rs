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

trait State<'a>: std::fmt::Debug  {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData> + 'a>);
    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData> + 'a>);
    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData> + 'a>);
}

#[derive(Debug)]
struct Init {}
impl<'a> State<'a> for Init {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        (Box::new(InitialLink { link }), Box::new(empty()))
    }

    fn on_trace(self: Box<Self>, _trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        panic!("Init: trace before initial link");
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("Init: stream ended, expected link");
    }
}

#[derive(Debug)]
struct InitialLink<'a> {
    link: &'a LinkData
}
impl<'a> State<'a> for InitialLink<'a> {
    fn on_link(self: Box<Self>, _link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: no trace on initial link");
    }

    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        (Box::new(InitialLinkWithTrace { link: self.link, trace }), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: stream ended, expected trace");
    }
}

#[derive(Debug)]
struct InitialLinkWithTrace<'a> {
    link: &'a LinkData,
    trace: &'a TraceData
}
impl<'a> State<'a> for InitialLinkWithTrace<'a> {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_ne!(self.link.link_id, link.link_id);
        let mut intermediates = LinkedList::new();
        intermediates.push_back(link);
        (Box::new(IntermediateLinkAfterInitial { initial_link: self.link, trace: self.trace, intermediates }), Box::new(empty()))
    }

    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(trace.link_id, self.trace.link_id);
        assert_eq!(trace.link_id, self.link.link_id);

        let delta_t = trace.timestamp - self.trace.timestamp;
        let delta_fraction = trace.traversed_in_travel_direction_fraction as f64 - self.trace.traversed_in_travel_direction_fraction as f64;
        let t_pre = (delta_t as f64 * self.trace.traversed_in_travel_direction_fraction as f64 / delta_fraction) as u64;

        debug_assert!(self.trace.timestamp > t_pre, "timestamp underflow");
        let entry_timestamp = self.trace.timestamp - t_pre;

        (Box::new(LinkWithEntryTimestampAndTrace { link: self.link, last_trace: trace, entry_timestamp, quality: delta_fraction }), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLinkWithTrace: stream ended, expected link or trace");
    }
}

#[derive(Debug, Clone)]
struct IntermediateLinkAfterInitial<'a> {
    initial_link: &'a LinkData,
    trace: &'a TraceData,
    intermediates: LinkedList<&'a LinkData>
}
impl<'a> State<'a> for IntermediateLinkAfterInitial<'a> {
    fn on_link(mut self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push_back(link);
        (self, Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData> + 'a>) {
        let link = self.intermediates.pop_back().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let initial_timestamp = self.trace.timestamp;
        let delta_t = trace.timestamp - initial_timestamp;
        let fraction_after_initial_trace = 1.0 - self.trace.traversed_in_travel_direction_fraction as f64;
        let length_after_initial_trace = (fraction_after_initial_trace * self.initial_link.length as f64) as u32;
        let length_before_initial_trace = self.initial_link.length - length_after_initial_trace;
        let intermediate_total_length: u32 = self.intermediates.iter().map(|&&LinkData { length, .. }| length).sum();
        let length_before_current_trace = (trace.traversed_in_travel_direction_fraction as f64 * link.length as f64) as u32;
        let total_length = length_after_initial_trace + intermediate_total_length + length_before_current_trace;

        let velocity = total_length as f64 / delta_t as f64;
        let time_before_initial = (length_before_initial_trace as f64 / velocity) as u64;
        debug_assert!(time_before_initial < initial_timestamp);
        let link_entered_timestamp = initial_timestamp - time_before_initial;
        let estimate_quality = (length_after_initial_trace as f64 / total_length as f64 * fraction_after_initial_trace) as f32;

        let output = once(LinkSpeedData { link_id: self.initial_link.link_id, link_entered_timestamp, estimate_quality, velocity: (velocity * 3.6) as f32  });

        let output = output.chain(self.intermediates.into_iter().scan(length_after_initial_trace, move |state, link| {
            let link_entered_timestamp = initial_timestamp + (*state as f64 / velocity) as u64;
            *state = *state + link.length;
            let estimate_quality = (link.length as f64 / total_length as f64) as f32;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality, velocity: (velocity * 3.6) as f32 })
        }));

        let entry_timestamp = initial_timestamp + ((length_after_initial_trace + intermediate_total_length) as f64 / velocity) as u64;
        let quality = length_before_current_trace as f64 / total_length as f64 * trace.traversed_in_travel_direction_fraction as f64;

        (Box::new(LinkWithEntryTimestampAndTrace { link: link, last_trace: trace, entry_timestamp, quality }), Box::new(output))
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("IntermediateLinkAfterInitial: stream ended, expected link or trace");
    }
}

#[derive(Debug, Clone)]
struct LinkWithEntryTimestampAndTrace<'a> {
    link: &'a LinkData,
    entry_timestamp: u64,
    last_trace: &'a TraceData,
    quality: f64
}
impl<'a> State<'a> for LinkWithEntryTimestampAndTrace<'a> {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        let mut intermediates = LinkedList::new();
        intermediates.push_back(link);
        (Box::new(IntermediateLink { last_link_with_trace: self, intermediates }), Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        self.quality += (trace.traversed_in_travel_direction_fraction - self.last_trace.traversed_in_travel_direction_fraction) as f64;
        self.last_trace = trace;
        (self.clone(), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        let delta_t = self.last_trace.timestamp - self.entry_timestamp;
        let delta_s = self.last_trace.traversed_in_travel_direction_fraction as f64 * self.link.length as f64;
        let velocity = delta_s / delta_t as f64;
        Box::new(once(LinkSpeedData { link_id: self.link.link_id, link_entered_timestamp: self.entry_timestamp, estimate_quality: self.quality as f32, velocity: (velocity * 3.6) as f32  }))
    }
}

#[derive(Debug, Clone)]
struct IntermediateLink<'a> {
    last_link_with_trace: Box<LinkWithEntryTimestampAndTrace<'a>>,
    intermediates: LinkedList<&'a LinkData>
}
impl<'a> State<'a> for IntermediateLink<'a> {
    fn on_link(mut self: Box<Self>, link: &'a LinkData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push_back(link);
        (self, Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<State + 'a>, Box<Iterator<Item = LinkSpeedData> + 'a>) {
        let link = self.intermediates.pop_back().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let previous_timestamp = self.last_link_with_trace.last_trace.timestamp;
        let delta_t = trace.timestamp - previous_timestamp;
        let fraction_after_last_trace = 1.0 - self.last_link_with_trace.last_trace.traversed_in_travel_direction_fraction as f64;
        let length_after_last_trace = (fraction_after_last_trace * self.last_link_with_trace.link.length as f64) as u32;
        let intermediate_total_length: u32 = self.intermediates.iter().map(|&&LinkData { length, .. }| length).sum();
        let length_before_current_trace = (trace.traversed_in_travel_direction_fraction as f64 * link.length as f64) as u32;
        let total_length = length_after_last_trace + intermediate_total_length + length_before_current_trace;

        let velocity = total_length as f64 / delta_t as f64;

        let time_on_link_after_last_trace = (velocity / length_after_last_trace as f64) as u64;
        let delta_t_link = self.last_link_with_trace.last_trace.timestamp - self.last_link_with_trace.entry_timestamp + time_on_link_after_last_trace;
        let link_velocity = self.last_link_with_trace.link.length as f64 / delta_t_link as f64;
        let estimate_quality = ((length_after_last_trace as f64 / total_length as f64 * fraction_after_last_trace) + self.last_link_with_trace.quality) as f32;

        let output = once(LinkSpeedData { link_id: self.last_link_with_trace.link.link_id, link_entered_timestamp: self.last_link_with_trace.entry_timestamp, estimate_quality, velocity: (link_velocity * 3.6) as f32 });

        let output = output.chain(self.intermediates.into_iter().scan(length_after_last_trace, move |state, link| {
            let link_entered_timestamp = previous_timestamp + (*state as f64 / velocity) as u64;
            *state = *state + link.length;
            let estimate_quality = (link.length as f64 / total_length as f64) as f32;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality, velocity: (velocity * 3.6) as f32 })
        }));

        let entry_timestamp = previous_timestamp + ((length_after_last_trace + intermediate_total_length) as f64 / velocity) as u64;
        let quality = length_before_current_trace as f64 / total_length as f64 * trace.traversed_in_travel_direction_fraction as f64;

        (Box::new(LinkWithEntryTimestampAndTrace { link: link, last_trace: trace, entry_timestamp, quality }), Box::new(output))
    }

    fn on_done(self: Box<Self>) -> (Box<Iterator<Item = LinkSpeedData>>) {
        panic!("IntermediateLink: stream ended, expected link or trace");
    }
}

#[derive(Debug)]
enum Event<'a> {
    Link(&'a LinkData),
    Trace(&'a TraceData),
}

#[derive(Debug)]
enum IteratorToPoll {
    Links,
    Traces,
}

struct StateMachine<'a> {
    state: Option<Box<State<'a> + 'a>>,
    links: Box<Iterator<Item = &'a LinkData> + 'a>,
    traces: Box<Iterator<Item = &'a TraceData> + 'a>,
    iterator_to_poll: IteratorToPoll,
    next_link: Option<&'a LinkData>,
    next_trace: &'a TraceData,
    done: bool,
    output_iter: Box<Iterator<Item = LinkSpeedData> + 'a>
}

impl<'a> StateMachine<'a> {
    fn new(mut links: Box<Iterator<Item = &'a LinkData> + 'a>, mut traces: Box<Iterator<Item = &'a TraceData> + 'a>) -> Result<StateMachine<'a>, &'static str> {
        let next_link = links.next().ok_or("no links given")?;
        let next_trace = traces.next().ok_or("no traces given")?;

        Ok(StateMachine {
            state: Some(Box::new(Init {})),
            links: links,
            traces: traces,
            iterator_to_poll: IteratorToPoll::Links,
            next_link: Some(next_link),
            next_trace,
            done: false,
            output_iter: Box::new(empty())
        })
    }

    fn next_event(&mut self) -> Option<Event<'a>> {
        if self.done { return None }

        match self.iterator_to_poll {
            IteratorToPoll::Links => {
                let result = Some(Event::Link(self.next_link.unwrap()));

                if self.next_link.unwrap().link_id == self.next_trace.link_id {
                    self.iterator_to_poll = IteratorToPoll::Traces;
                } else {
                    self.iterator_to_poll = IteratorToPoll::Links;
                }

                self.next_link = self.links.next();

                result
            },
            IteratorToPoll::Traces => {
                let result = Some(Event::Trace(self.next_trace));

                match self.traces.next() {
                    Some(trace) => {
                        if self.next_trace.link_id == trace.link_id {
                            self.iterator_to_poll = IteratorToPoll::Traces;
                        } else {
                            self.iterator_to_poll = IteratorToPoll::Links;
                        }

                        self.next_trace = trace;
                    },
                    None => {
                        debug_assert_eq!(self.links.next(), None);
                        self.done = true;
                    },
                }

                result
            },
        }
    }
}

impl<'a> Iterator for StateMachine<'a> {
    type Item = LinkSpeedData;

    fn next(&mut self) -> Option<LinkSpeedData> {
        loop {
            match self.output_iter.next() {
                Some(out) => return Some(out),
                None => {
                    let current_state = std::mem::replace(&mut self.state, None);

                    match self.next_event() {
                        Some(event) => {
                            let (state, iter) = match event {
                                Event::Link(link) => current_state.unwrap().on_link(link),
                                Event::Trace(trace) => current_state.unwrap().on_trace(trace)
                            };
                            self.state = Some(state);
                            self.output_iter = iter;
                        },
                        None => {
                            match current_state {
                                Some(state) => {
                                    self.output_iter = state.on_done();
                                    self.state = None;
                                },
                                None => return None
                            }
                        },
                    }
                },
            }
        }
    }
}

fn calculate<'a>(links: Box<Iterator<Item = &'a LinkData> + 'a>, traces: Box<Iterator<Item = &'a TraceData> + 'a>) -> Result<Vec<LinkSpeedData>, &'static str> {
    let output_iter = StateMachine::new(links, traces)?;
    Ok(output_iter.collect())
}

fn main() {
    let links = vec![];
    let traces = vec![];
    calculate(Box::new(links.iter()), Box::new(traces.iter())).expect("no trace points");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_for_empty_errors() {
        let links = vec![];
        let traces = vec![];
        assert!(calculate(Box::new(links.iter()), Box::new(traces.iter())).is_err());
    }

    #[test]
    fn two_points_one_link() {
        let links = vec![LinkData { link_id: 1, length: 10000, speed_limit: 50 }];
        let traces = vec![
            TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.1 },
            TraceData { timestamp: 101000, link_id: 1, traversed_in_travel_direction_fraction: 0.9 }
        ];
        assert_eq!(calculate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
            vec![LinkSpeedData { link_id: 1, link_entered_timestamp: 99875, estimate_quality: 0.9 - 0.1, velocity: 28.8 }]);
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
        assert_eq!(calculate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
            vec![
                LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.125, velocity: 72.0 },
                LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 0.375, velocity: 72.0 }
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
        assert_eq!(calculate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
            vec![
                LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.0625, velocity: 72.0 },
                LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 0.75, velocity: 72.0 },
                LinkSpeedData { link_id: 3, link_entered_timestamp: 101750, estimate_quality: 0.0625, velocity: 72.0 },
            ]);
    }

    #[test]
    fn three_points_three_links() {
        let links = vec![
            LinkData { link_id: 1, length: 10000, speed_limit: 80 },
            LinkData { link_id: 2, length: 30000, speed_limit: 80 },
            LinkData { link_id: 3, length: 10000, speed_limit: 80 },
        ];
        let traces = vec![
            TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.5 },
            TraceData { timestamp: 100250, link_id: 1, traversed_in_travel_direction_fraction: 1.0 },
            TraceData { timestamp: 102000, link_id: 3, traversed_in_travel_direction_fraction: 0.5 }
        ];
        assert_eq!(calculate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
            vec![
                LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.5, velocity: 72.0 },
                LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 30000.0 / 35000.0, velocity: 72.0 },
                LinkSpeedData { link_id: 3, link_entered_timestamp: 101750, estimate_quality: 0.5 * 5000.0 / 35000.0, velocity: 72.0 },
            ]);
    }
}
