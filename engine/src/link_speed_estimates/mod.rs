mod event_iterator;
mod link_speed_estimator;

use self::event_iterator::{EventIterator, Event};
use self::link_speed_estimator::LinkSpeedEstimator;

#[derive(Debug)]
pub struct TraceData {
    pub timestamp: u64, // [ms]
    pub link_id: u64,
    pub traversed_in_travel_direction_fraction: f32 // [0.0, 1.0]
}

#[derive(Debug, PartialEq)]
pub struct LinkData {
    pub link_id: u64,
    pub length: u32, // [mm]
    pub speed_limit: u32, // [km/s]
}

impl LinkData {
    pub fn free_flow_traversal_time(&self) -> f64 {
        f64::from(self.length) * 3.6 / f64::from(self.speed_limit)
    }
}

#[derive(Debug, PartialEq)]
pub struct LinkSpeedData {
    pub link_id: u64,
    pub link_entered_timestamp: u64,
    pub estimate_quality: f32,
    pub velocity: f32
}

pub fn estimate_iter<'a>(links: Box<dyn Iterator<Item = &'a LinkData> + 'a>, traces: Box<dyn Iterator<Item = &'a TraceData> + 'a>) -> Result<impl Iterator<Item = LinkSpeedData> + 'a, &'static str> {
    let output_iter = LinkSpeedEstimator::new(EventIterator::new(links, traces)?);
    Ok(output_iter)
}
