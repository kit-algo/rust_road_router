use super::*;
use math::RangeExtensions;

#[derive(Debug, Clone)]
pub struct Linked<'a> {
    first: &'a Shortcut,
    second: &'a Shortcut,
}

impl<'a> Linked<'a> {
    pub fn new(first: &'a Shortcut, second: &'a Shortcut) -> Self {
        Linked { first, second }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        let first_edge_value = self.first.evaluate(departure, shortcut_graph);
        first_edge_value + self.second.evaluate((departure + first_edge_value) % period(), shortcut_graph)
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        if !self.is_valid_path_during(&(0..period())) { return (INFINITY, INFINITY); }
        let (first_min, first_max) = self.first.bounds();
        debug_assert!(first_min < INFINITY);
        debug_assert!(first_max < INFINITY);
        let (second_min, second_max) = self.second.bounds();
        debug_assert!(second_min < INFINITY);
        debug_assert!(second_max < INFINITY);

        debug_assert!(first_min + second_min < INFINITY);
        debug_assert!(first_max + second_max < INFINITY);
        (first_min + second_min, first_max + second_max)
    }

    pub fn bounds_for(&self, range: &Range<Timestamp>) -> Option<(Weight, Weight)> {
        if range.start == range.end { return None }

        if let Some((first_range, second_range)) = self.ranges_for_second(range) {
            let (in_min, in_max) = self.first.bounds_for(range).unwrap();

            match (self.second.bounds_for(&first_range), self.second.bounds_for(&second_range)) {
                (Some((out_first_min, out_first_max)), Some((out_second_min, out_second_max))) =>
                    Some((in_min + min(out_first_min, out_second_min), in_max + max(out_first_max, out_second_max))),
                (Some((out_first_min, out_first_max)), None) =>
                    Some((in_min + out_first_min, in_max + out_first_max)),
                (None, Some((out_second_min, out_second_max))) =>
                    Some((in_min + out_second_min, in_max + out_second_max)),
                (None, None) =>
                    None
            }
        } else {
            return Some((INFINITY, INFINITY));
        }
    }

    pub fn is_valid_path_during(&self, range: &Range<Timestamp>) -> bool {
        if let Some((first_range, second_range)) = self.ranges_for_second(range) {
            self.second.is_valid_path_during(&first_range) && self.second.is_valid_path_during(&second_range)
        } else {
            false
        }
    }

    pub fn ranges_for_second(&self, range: &Range<Timestamp>) -> Option<(Range<Timestamp>, Range<Timestamp>)> {
        debug_assert!(range.start <= period());
        debug_assert!(range.end <= period());
        debug_assert!(range.start <= range.end);
        let (in_min, in_max) = self.first.bounds_for(range)?;
        if in_min >= INFINITY || in_max >= INFINITY { return None }
        let (first_range, mut second_range) = (range.start + in_min .. range.end + in_max).split(period());
        second_range.start -= period();
        second_range.end -= period();
        debug_assert!(first_range.start <= period());
        debug_assert!(first_range.end <= period());
        debug_assert!(first_range.start <= range.end);
        debug_assert!(second_range.start <= period());
        debug_assert!(second_range.end <= period());
        debug_assert!(second_range.start <= range.end);
        Some((first_range, second_range))
    }

    pub fn debug_to_s(&self, shortcut_graph: &ShortcutGraph, indent: usize) -> String {
        format!("Linked:\n{}first: {}\n{}second: {}",
            String::from(" ").repeat(indent * 2),
            self.first.debug_to_s(shortcut_graph, indent + 1),
            String::from(" ").repeat(indent * 2),
            self.second.debug_to_s(shortcut_graph, indent + 1))
    }
}
