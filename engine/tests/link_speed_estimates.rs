extern crate bmw_routing_engine;

use bmw_routing_engine::link_speed_estimates::*;

#[test]
fn check_for_empty_errors() {
    let links = vec![];
    let traces = vec![];
    assert!(estimate(Box::new(links.iter()), Box::new(traces.iter())).is_err());
}

#[test]
fn two_points_one_link() {
    let links = vec![LinkData { link_id: 1, length: 10000, speed_limit: 50 }];
    let traces = vec![
        TraceData { timestamp: 100000, link_id: 1, traversed_in_travel_direction_fraction: 0.1 },
        TraceData { timestamp: 101000, link_id: 1, traversed_in_travel_direction_fraction: 0.9 }
    ];
    assert_eq!(estimate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
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
    assert_eq!(estimate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
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
    assert_eq!(estimate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
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
    assert_eq!(estimate(Box::new(links.iter()), Box::new(traces.iter())).unwrap(),
        vec![
            LinkSpeedData { link_id: 1, link_entered_timestamp: 99750, estimate_quality: 0.5, velocity: 72.0 },
            LinkSpeedData { link_id: 2, link_entered_timestamp: 100250, estimate_quality: 30000.0 / 35000.0, velocity: 72.0 },
            LinkSpeedData { link_id: 3, link_entered_timestamp: 101750, estimate_quality: 0.5 * 5000.0 / 35000.0, velocity: 72.0 },
        ]);
}
