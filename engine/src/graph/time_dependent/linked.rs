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

    pub fn bounds_for(&self, range: &Range<Timestamp>) -> (Weight, Weight) {
        let (in_min, in_max) = self.first.bounds_for(range);
        if in_min >= INFINITY {
            return (INFINITY, INFINITY);
        }
        let (first_range, mut second_range) = (range.start + in_min .. range.end + in_max).split(period());
        second_range.start -= period();
        second_range.end -= period();
        let (out_first_min, out_first_max) = self.second.bounds_for(&first_range);
        let (out_second_min, out_second_max) = self.second.bounds_for(&second_range);
        (in_min + min(out_first_min, out_second_min), in_max + max(out_first_max, out_second_max))
    }

    pub fn is_valid_path_during(&self, range: &Range<Timestamp>) -> bool {
        if !self.first.is_valid_path_during(range) { return false }
        let (in_min, in_max) = self.first.bounds_for(range);
        debug_assert!(in_min < INFINITY);
        debug_assert!(in_max < INFINITY);
        let (first_range, mut second_range) = (range.start + in_min .. range.end + in_max).split(period());
        second_range.start -= period();
        second_range.end -= period();
        self.second.is_valid_path_during(&first_range) && self.second.is_valid_path_during(&second_range)
    }

    pub fn debug_to_s(&self, shortcut_graph: &ShortcutGraph, indent: usize) -> String {
        format!("Linked:\n{}first: {}\n{}second: {}",
            String::from(" ").repeat(indent * 2),
            self.first.debug_to_s(shortcut_graph, indent + 1),
            String::from(" ").repeat(indent * 2),
            self.second.debug_to_s(shortcut_graph, indent + 1))
    }
}
