use std::iter::{empty, once};
use std::fmt::Debug;
use std::mem::replace;
use super::*;

trait State<'a>: Debug  {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData> + 'a>);
    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData> + 'a>);
    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData> + 'a>);
}

#[derive(Debug)]
struct Init {}
impl<'a> State<'a> for Init {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        (Box::new(InitialLink { link }), Box::new(empty()))
    }

    fn on_trace(self: Box<Self>, _trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("Init: trace before initial link");
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("Init: stream ended, expected link");
    }
}

#[derive(Debug)]
struct InitialLink<'a> {
    link: &'a LinkData
}
impl<'a> State<'a> for InitialLink<'a> {
    fn on_link(self: Box<Self>, _link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: no trace on initial link");
    }

    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        (Box::new(InitialLinkWithTrace { link: self.link, trace }), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLink: stream ended, expected trace");
    }
}

#[derive(Debug)]
struct InitialLinkWithTrace<'a> {
    link: &'a LinkData,
    trace: &'a TraceData
}
impl<'a> State<'a> for InitialLinkWithTrace<'a> {
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        assert_ne!(self.link.link_id, link.link_id);
        let mut intermediates = Vec::new();
        intermediates.push(link);
        (Box::new(IntermediateLinkAfterInitial { initial_link: self.link, trace: self.trace, intermediates }), Box::new(empty()))
    }

    fn on_trace(self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        assert_eq!(trace.link_id, self.trace.link_id);
        assert_eq!(trace.link_id, self.link.link_id);

        let delta_t = trace.timestamp - self.trace.timestamp;
        let delta_fraction = f64::from(trace.traversed_in_travel_direction_fraction) - f64::from(self.trace.traversed_in_travel_direction_fraction);
        let t_pre = (delta_t as f64 * f64::from(self.trace.traversed_in_travel_direction_fraction) / delta_fraction) as u64;

        debug_assert!(self.trace.timestamp > t_pre, "timestamp underflow");
        let entry_timestamp = self.trace.timestamp - t_pre;

        (Box::new(LinkWithEntryTimestampAndTrace { link: self.link, last_trace: trace, entry_timestamp, quality: delta_fraction }), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("InitialLinkWithTrace: stream ended, expected link or trace");
    }
}

#[derive(Debug, Clone)]
struct IntermediateLinkAfterInitial<'a> {
    initial_link: &'a LinkData,
    trace: &'a TraceData,
    intermediates: Vec<&'a LinkData>
}
impl<'a> State<'a> for IntermediateLinkAfterInitial<'a> {
    fn on_link(mut self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push(link);
        (self, Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData> + 'a>) {
        let link = self.intermediates.pop().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let initial_timestamp = self.trace.timestamp;
        let delta_t = trace.timestamp - initial_timestamp;

        let fraction_after_initial_trace = 1.0 - f64::from(self.trace.traversed_in_travel_direction_fraction);
        let length_after_initial_trace = (fraction_after_initial_trace * f64::from(self.initial_link.length)) as u32;
        let freeflow_time_after_initial_trace = self.initial_link.free_flow_traversal_time() * fraction_after_initial_trace;
        let length_before_initial_trace = self.initial_link.length - length_after_initial_trace;

        let intermediate_total_length: u32 = self.intermediates.iter().map(|&&LinkData { length, .. }| length).sum();
        let intermediate_total_freeflow_time: f64 = self.intermediates.iter().map(|link| link.free_flow_traversal_time()).sum();

        let length_before_current_trace = (f64::from(trace.traversed_in_travel_direction_fraction) * f64::from(link.length)) as u32;
        let freeflow_time_before_current_trace = link.free_flow_traversal_time() * f64::from(trace.traversed_in_travel_direction_fraction);

        let total_length = length_after_initial_trace + intermediate_total_length + length_before_current_trace;
        let total_freeflow_time = freeflow_time_after_initial_trace + intermediate_total_freeflow_time + freeflow_time_before_current_trace;
        let velocity_factor = total_freeflow_time / delta_t as f64;

        let initial_link_velocity = f64::from(self.initial_link.speed_limit) * velocity_factor;
        let time_before_initial = (f64::from(length_before_initial_trace) * 3.6 / initial_link_velocity) as u64;
        let next_entry = (f64::from(length_after_initial_trace) * 3.6 / initial_link_velocity) as u64 + initial_timestamp;
        debug_assert!(time_before_initial < initial_timestamp);
        let link_entered_timestamp = initial_timestamp - time_before_initial;
        let estimate_quality = (f64::from(length_after_initial_trace) / f64::from(total_length) * fraction_after_initial_trace) as f32;

        let output = once(LinkSpeedData { link_id: self.initial_link.link_id, link_entered_timestamp, estimate_quality, velocity: initial_link_velocity as f32  });

        let output = output.chain(self.intermediates.into_iter().scan(next_entry, move |state, link| {
            let velocity = f64::from(link.speed_limit) * velocity_factor;
            let traversal_time = (f64::from(link.length) * 3.6 / velocity) as u64;
            let link_entered_timestamp = *state;
            *state += traversal_time;
            let estimate_quality = (f64::from(link.length) / f64::from(total_length)) as f32;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality, velocity: velocity as f32 })
        }));

        let entry_timestamp = trace.timestamp - (f64::from(length_before_current_trace) * 3.6 / (f64::from(link.speed_limit) * velocity_factor)) as u64;
        let quality = f64::from(length_before_current_trace) / f64::from(total_length) * f64::from(trace.traversed_in_travel_direction_fraction);

        (Box::new(LinkWithEntryTimestampAndTrace { link, last_trace: trace, entry_timestamp, quality }), Box::new(output))
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
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
    fn on_link(self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        let mut intermediates = Vec::new();
        intermediates.push(link);
        (Box::new(IntermediateLink { last_link_with_trace: self, intermediates }), Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        assert_eq!(self.link.link_id, trace.link_id);
        self.quality += f64::from(trace.traversed_in_travel_direction_fraction - self.last_trace.traversed_in_travel_direction_fraction);
        self.last_trace = trace;
        (self.clone(), Box::new(empty()))
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
        let delta_t = self.last_trace.timestamp - self.entry_timestamp;
        let delta_s = f64::from(self.last_trace.traversed_in_travel_direction_fraction) * f64::from(self.link.length);
        let velocity = delta_s / delta_t as f64;
        Box::new(once(LinkSpeedData { link_id: self.link.link_id, link_entered_timestamp: self.entry_timestamp, estimate_quality: self.quality as f32, velocity: (velocity * 3.6) as f32  }))
    }
}

#[derive(Debug, Clone)]
struct IntermediateLink<'a> {
    last_link_with_trace: Box<LinkWithEntryTimestampAndTrace<'a>>,
    intermediates: Vec<&'a LinkData>
}
impl<'a> State<'a> for IntermediateLink<'a> {
    fn on_link(mut self: Box<Self>, link: &'a LinkData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData>>) {
        self.intermediates.push(link);
        (self, Box::new(empty()))
    }

    fn on_trace(mut self: Box<Self>, trace: &'a TraceData) -> (Box<dyn State + 'a>, Box<dyn Iterator<Item = LinkSpeedData> + 'a>) {
        let link = self.intermediates.pop().unwrap();
        assert_eq!(link.link_id, trace.link_id);

        let previous_timestamp = self.last_link_with_trace.last_trace.timestamp;
        let delta_t = trace.timestamp - previous_timestamp;

        let fraction_after_last_trace = 1.0 - f64::from(self.last_link_with_trace.last_trace.traversed_in_travel_direction_fraction);
        let length_after_last_trace = (fraction_after_last_trace * f64::from(self.last_link_with_trace.link.length)) as u32;
        let freeflow_time_after_last_trace = self.last_link_with_trace.link.free_flow_traversal_time() * fraction_after_last_trace;

        let intermediate_total_length: u32 = self.intermediates.iter().map(|&&LinkData { length, .. }| length).sum();
        let intermediate_total_freeflow_time: f64 = self.intermediates.iter().map(|link| link.free_flow_traversal_time()).sum();

        let length_before_current_trace = (f64::from(trace.traversed_in_travel_direction_fraction) * f64::from(link.length)) as u32;
        let freeflow_time_before_current_trace = link.free_flow_traversal_time() * f64::from(trace.traversed_in_travel_direction_fraction);

        let total_length = length_after_last_trace + intermediate_total_length + length_before_current_trace;
        let total_freeflow_time = freeflow_time_after_last_trace + intermediate_total_freeflow_time + freeflow_time_before_current_trace;
        let velocity_factor = total_freeflow_time / delta_t as f64;

        let last_link_velocity = f64::from(self.last_link_with_trace.link.speed_limit) * velocity_factor;
        let time_on_link_after_last_trace = (f64::from(length_after_last_trace) * 3.6 / last_link_velocity) as u64;
        let next_entry = self.last_link_with_trace.last_trace.timestamp + time_on_link_after_last_trace;
        let delta_t_link = self.last_link_with_trace.last_trace.timestamp - self.last_link_with_trace.entry_timestamp + time_on_link_after_last_trace;
        let link_velocity = f64::from(self.last_link_with_trace.link.length) / delta_t_link as f64;
        let estimate_quality = ((f64::from(length_after_last_trace) / f64::from(total_length) * fraction_after_last_trace) + self.last_link_with_trace.quality) as f32;

        let output = once(LinkSpeedData { link_id: self.last_link_with_trace.link.link_id, link_entered_timestamp: self.last_link_with_trace.entry_timestamp, estimate_quality, velocity: (link_velocity * 3.6) as f32 });

        let output = output.chain(self.intermediates.into_iter().scan(next_entry, move |state, link| {
            let velocity = f64::from(link.speed_limit) * velocity_factor;
            let traversal_time = (f64::from(link.length) * 3.6 / velocity) as u64;
            let link_entered_timestamp = *state;
            *state += traversal_time;
            let estimate_quality = (f64::from(link.length) / f64::from(total_length)) as f32;
            Some(LinkSpeedData { link_id: link.link_id, link_entered_timestamp, estimate_quality, velocity: velocity as f32 })
        }));

        let entry_timestamp = trace.timestamp - (f64::from(length_before_current_trace) * 3.6 / (f64::from(link.speed_limit) * velocity_factor)) as u64;
        let quality = f64::from(length_before_current_trace) / f64::from(total_length) * f64::from(trace.traversed_in_travel_direction_fraction);

        (Box::new(LinkWithEntryTimestampAndTrace { link, last_trace: trace, entry_timestamp, quality }), Box::new(output))
    }

    fn on_done(self: Box<Self>) -> (Box<dyn Iterator<Item = LinkSpeedData>>) {
        panic!("IntermediateLink: stream ended, expected link or trace");
    }
}

pub struct LinkSpeedEstimator<'a> {
    state: Option<Box<dyn State<'a> + 'a>>,
    event_iterator: EventIterator<'a>,
    output_iterator: Box<dyn Iterator<Item = LinkSpeedData> + 'a>,
}

impl<'a> LinkSpeedEstimator<'a> {
    pub fn new(events: EventIterator<'a>) -> LinkSpeedEstimator<'a> {
        LinkSpeedEstimator {
            state: Some(Box::new(Init {})),
            event_iterator: events,
            output_iterator: Box::new(empty())
        }
    }
}

impl<'a> Iterator for LinkSpeedEstimator<'a> {
    type Item = LinkSpeedData;

    fn next(&mut self) -> Option<LinkSpeedData> {
        loop {
            match self.output_iterator.next() {
                Some(out) => return Some(out),
                None => {
                    let current_state = replace(&mut self.state, None);

                    match self.event_iterator.next() {
                        Some(event) => {
                            let (state, iter) = match event {
                                Event::Link(link) => current_state.unwrap().on_link(link),
                                Event::Trace(trace) => current_state.unwrap().on_trace(trace)
                            };
                            self.state = Some(state);
                            self.output_iterator = iter;
                        },
                        None => {
                            match current_state {
                                Some(state) => {
                                    self.output_iterator = state.on_done();
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
