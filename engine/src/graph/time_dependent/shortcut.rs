use super::*;
use rank_select_map::BitVec;
use math::RangeExtensions;

use std::iter::Once;
use std::slice::Iter as SliceIter;

#[derive(Debug, Clone)]
struct Bounds {
    lower: Weight,
    upper: Weight,
}

impl Default for Bounds {
    fn default() -> Self {
        Bounds { lower: INFINITY, upper: INFINITY }
    }
}

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: [(Bounds, ShortcutPaths); NUM_WINDOWS],
}

const NUM_WINDOWS: usize = 24;
#[inline]
pub fn window_size() -> Weight {
    period() / NUM_WINDOWS as u32
}

#[derive(Debug, Clone)]
enum ShortcutPaths {
    None,
    One(ShortcutData),
    Multi(Vec<ShortcutData>)
}

impl Default for ShortcutPaths {
    fn default() -> Self {
        ShortcutPaths::None
    }
}

impl Shortcut {
    pub fn new(source: Option<(EdgeId, PiecewiseLinearFunction)>) -> Shortcut {
        match source {
            Some((edge, plf)) => {
                let mut data: [(Bounds, ShortcutPaths); NUM_WINDOWS] = Default::default();
                for (paths, range) in data.iter_mut().zip((0..NUM_WINDOWS).map(Shortcut::window_time_range)) {
                    let (lower, upper) = plf.bounds_for(&range);
                    *paths = (Bounds { lower, upper }, ShortcutPaths::One(ShortcutData::new(ShortcutSource::OriginalEdge(edge))));
                }
                Shortcut { data }
            },
            None => Shortcut { data: Default::default() },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        // TODO clean up types
        let shortcut_data = ShortcutData::new(ShortcutSource::Shortcut(linked_ids.0, linked_ids.1));
        let linked = Linked::new(shortcut_graph.get_incoming(linked_ids.0), shortcut_graph.get_outgoing(linked_ids.1));

        for (i, (bounds, paths)) in self.data.iter_mut().enumerate() {
            let range = Shortcut::window_time_range(i);

            if !linked.is_valid_path_during(&range) {
                continue;
            }
            let (other_lower_bound, other_upper_bound) = linked.bounds_for(&range);

            if !paths.is_valid_path() {
                *bounds = Bounds { lower: other_lower_bound, upper: other_upper_bound };
                *paths = ShortcutPaths::One(shortcut_data);
                continue;
            }

            if bounds.lower >= other_upper_bound {
                *bounds = Bounds { lower: other_lower_bound, upper: other_upper_bound };
                *paths = ShortcutPaths::One(shortcut_data);
                continue;
            } else if other_lower_bound >= bounds.upper {
                continue;
            }

            debug_assert!(bounds.lower < INFINITY);
            debug_assert!(bounds.upper < INFINITY);
            debug_assert!(other_lower_bound < INFINITY);
            debug_assert!(other_upper_bound < INFINITY);
            bounds.lower = min(bounds.lower, other_lower_bound);
            bounds.upper = min(bounds.upper, other_upper_bound);
            match paths {
                ShortcutPaths::None => *paths = ShortcutPaths::One(shortcut_data),
                ShortcutPaths::One(data) => *paths = ShortcutPaths::Multi(vec![*data, shortcut_data]),
                ShortcutPaths::Multi(data) => {
                    data.push(shortcut_data);
                }
            }
        }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match &self.data[(departure / window_size()) as usize].1 {
            ShortcutPaths::None => INFINITY,
            ShortcutPaths::One(data) => data.evaluate(departure, shortcut_graph),
            ShortcutPaths::Multi(data) => {
                data.iter().map(|path| path.evaluate(departure, shortcut_graph)).min().unwrap()
            }
        }
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        self.data.iter()
            .map(|(bounds, _)| bounds)
            .fold((INFINITY, 0), |(acc_min, acc_max), Bounds { lower, upper }| (min(acc_min, *lower), max(acc_max, *upper)))
    }

    pub fn bounds_for(&self, range: &Range<Timestamp>) -> (Weight, Weight) {
        self.data[Shortcut::time_range_to_window_range(range)].iter()
            .map(|(bounds, _)| bounds)
            .fold((INFINITY, 0), |(acc_min, acc_max), Bounds { lower, upper }| (min(acc_min, *lower), max(acc_max, *upper)))
    }

    pub fn remove_dominated(&mut self, shortcut_graph: &ShortcutGraph) {
        for (i, (bounds, paths)) in self.data.iter_mut().enumerate() {
            if let ShortcutPaths::Multi(data) = paths {
                data.retain(|path| path.bounds_for(&Shortcut::window_time_range(i), shortcut_graph).0 <= bounds.upper);
                if data.len() == 1 {
                    *paths = ShortcutPaths::One(data[0]);
                } else if data.is_empty() {
                    *paths = ShortcutPaths::None;
                }
            }
        }
    }

    pub fn remove_dominated_by(&mut self, shortcut_graph: &ShortcutGraph, other: &Linked) {
        for (i, (bounds, paths)) in self.data.iter_mut().enumerate() {
            let range = Shortcut::window_time_range(i);
            let upper_bound = other.bounds_for(&range).1;
            match paths {
                ShortcutPaths::One(_) => {
                    if bounds.lower > upper_bound {
                        *paths = ShortcutPaths::None;
                    }
                },
                ShortcutPaths::Multi(data) => {
                    if bounds.upper > upper_bound {
                        data.retain(|path| path.bounds_for(&range, shortcut_graph).0 <= upper_bound);
                        *bounds = data.iter().map(|path| path.bounds_for(&range, shortcut_graph))
                            .fold(Bounds { lower: INFINITY, upper: INFINITY }, |acc, bounds|
                                Bounds { lower: min(acc.lower, bounds.0), upper: min(acc.upper, bounds.1) });
                        if data.len() == 1 {
                            *paths = ShortcutPaths::One(data[0]);
                        } else if data.is_empty() {
                            *paths = ShortcutPaths::None;
                        }
                    }
                }
                _ => (),
            }
        }
    }

    pub fn num_path_segments(&self) -> usize {
        self.data.iter().map(|(_, p)| p).map(ShortcutPaths::num_path_segments).max().unwrap()
    }

    pub fn is_valid_path_during(&self, range: &Range<Timestamp>) -> bool {
        self.data[Shortcut::time_range_to_window_range(range)].iter().all(|(_, paths)| paths.is_valid_path())
    }

    pub fn unpack(&self, range: &Range<Timestamp>, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        for (i, (_, paths)) in self.data[Shortcut::time_range_to_window_range(range)].iter().enumerate() {
            paths.unpack(&Shortcut::window_time_range(i).intersection(range), shortcut_graph, unpacked_shortcuts, original_edges);
        }
    }

    fn time_range_to_window_range(range: &Range<Timestamp>) -> Range<usize> {
        (range.start / window_size()) as usize .. ((range.end + window_size() - 1) / window_size()) as usize
    }

    fn window_time_range(window: usize) -> Range<Timestamp> {
        window as Timestamp * window_size() .. (window + 1) as Timestamp * window_size()
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        println!("{:?}", self.data);
        let mut s = String::from("Shortcut: ");

        for (i, (_, paths)) in self.data.iter().enumerate() {
            s.push('\n');
            for _ in 0..indent {
                s.push(' ');
                s.push(' ');
            }
            s = s + &format!("{}: {}", i, paths.debug_to_s(shortcut_graph, indent + 1));
        }

        s
    }
}

impl ShortcutPaths {
    // TODO for range
    pub fn unpack(&self, range: &Range<Timestamp>, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        for source in PathSegmentIter::new(self) {
            source.unpack(range, shortcut_graph, unpacked_shortcuts, original_edges);
        }
    }

    pub fn num_path_segments(&self) -> usize {
        match self {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_) => 1,
            ShortcutPaths::Multi(data) => data.len()
        }
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_path_segments() > 0
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        let mut s = String::from("ShortcutPaths: ");
        for source in PathSegmentIter::new(self) {
            s.push('\n');
            for _ in 0..indent {
                s.push(' ');
                s.push(' ');
            }
            s = s + &source.debug_to_s(shortcut_graph, indent + 1);
        }
        s
    }
}

#[derive(Debug)]
enum PathSegmentIter<'a> {
    None,
    One(Once<&'a ShortcutData>),
    Multi(SliceIter<'a, ShortcutData>)
}

impl<'a> PathSegmentIter<'a> {
    fn new(shortcut: &'a ShortcutPaths) -> PathSegmentIter<'a> {
        match shortcut {
            ShortcutPaths::None => PathSegmentIter::None,
            ShortcutPaths::One(data) => PathSegmentIter::One(once(data)),
            ShortcutPaths::Multi(data) => PathSegmentIter::Multi(data.iter())
        }
    }
}

impl<'a> Iterator for PathSegmentIter<'a> {
    type Item = (&'a ShortcutData);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PathSegmentIter::None => None,
            PathSegmentIter::One(iter) => iter.next(),
            PathSegmentIter::Multi(iter) => iter.next()
        }
    }
}
