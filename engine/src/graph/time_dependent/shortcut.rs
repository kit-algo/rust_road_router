use super::*;
use rank_select_map::BitVec;

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
    data: [ShortcutPaths; NUM_WINDOWS],
}

const NUM_WINDOWS: usize = 8;
#[inline]
pub fn window_size() -> Weight {
    period() / NUM_WINDOWS as u32
}

#[derive(Debug, Clone)]
enum ShortcutPaths {
    None,
    One(Bounds, ShortcutData),
    Multi(Bounds, Vec<ShortcutData>)
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
                let mut data: [ShortcutPaths; NUM_WINDOWS] = Default::default();
                for (paths, range) in data.iter_mut().zip((0..NUM_WINDOWS).map(Shortcut::window_time_range)) {
                    let (lower, upper) = plf.bounds_for(&range);
                    *paths = ShortcutPaths::One(Bounds { lower, upper }, ShortcutData::new(ShortcutSource::OriginalEdge(edge)));
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

        for (i, paths) in self.data.iter_mut().enumerate() {
            let range = Shortcut::window_time_range(i);

            if !linked.is_valid_path() {
                continue;
            }
            let (other_lower_bound, other_upper_bound) = linked.bounds_for(&range);

            if !paths.is_valid_path() {
                *paths = ShortcutPaths::One(Bounds { lower: other_lower_bound, upper: other_upper_bound }, shortcut_data);
                continue;
            }

            let (current_lower_bound, current_upper_bound) = paths.bounds();
            if current_lower_bound >= other_upper_bound {
                *paths = ShortcutPaths::One(Bounds { lower: other_lower_bound, upper: other_upper_bound }, shortcut_data);
                continue;
            } else if other_lower_bound >= current_upper_bound {
                continue;
            }

            debug_assert!(current_lower_bound < INFINITY);
            debug_assert!(current_upper_bound < INFINITY);
            debug_assert!(other_lower_bound < INFINITY);
            debug_assert!(other_upper_bound < INFINITY);
            let lower = min(current_lower_bound, other_lower_bound);
            let upper = min(current_upper_bound, other_upper_bound);
            match paths {
                ShortcutPaths::None => *paths = ShortcutPaths::One(Bounds { lower: other_lower_bound, upper: other_upper_bound }, shortcut_data),
                ShortcutPaths::One(_, data) => *paths = ShortcutPaths::Multi(Bounds { lower, upper }, vec![*data, shortcut_data]),
                ShortcutPaths::Multi(bounds, data) => {
                    data.push(shortcut_data);
                    *bounds = Bounds { lower, upper };
                }
            }
        }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match self.data[(departure / window_size()) as usize] {
            ShortcutPaths::None => INFINITY,
            ShortcutPaths::One(_, data) => data.evaluate(departure, shortcut_graph),
            ShortcutPaths::Multi(_, ref data) => {
                data.iter().map(|path| path.evaluate(departure, shortcut_graph)).min().unwrap()
            }
        }
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        self.data.iter()
            .map(ShortcutPaths::bounds)
            .fold((INFINITY, 0), |(acc_min, acc_max), (lower, upper)| (min(acc_min, lower), max(acc_max, upper)))
    }

    pub fn bounds_for(&self, range: &Range<Timestamp>) -> (Weight, Weight) {
        self.data[Shortcut::time_range_to_window_range(range)].iter()
            .map(ShortcutPaths::bounds)
            .fold((INFINITY, 0), |(acc_min, acc_max), (lower, upper)| (min(acc_min, lower), max(acc_max, upper)))
    }

    pub fn remove_dominated(&mut self, shortcut_graph: &ShortcutGraph) {
        for (i, mut paths) in self.data.iter_mut().enumerate() {
            if let ShortcutPaths::Multi(bounds, data) = &mut paths {
                data.retain(|path| path.bounds_for(&Shortcut::window_time_range(i), shortcut_graph).0 <= bounds.upper);
                if data.len() == 1 {
                    *paths = ShortcutPaths::One(bounds.clone(), data[0]);
                } else if data.is_empty() {
                    *paths = ShortcutPaths::None;
                }
            }
        }
    }

    pub fn remove_dominated_by(&mut self, shortcut_graph: &ShortcutGraph, other: &Linked) {
        for (i, mut paths) in self.data.iter_mut().enumerate() {
            let range = Shortcut::window_time_range(i);
            let upper_bound = other.bounds_for(&range).1;
            match &mut paths {
                ShortcutPaths::One(bounds, _) => {
                    if bounds.lower > upper_bound {
                        *paths = ShortcutPaths::None;
                    }
                },
                ShortcutPaths::Multi(bounds, data) => {
                    if bounds.upper > upper_bound {
                        data.retain(|path| path.bounds_for(&range, shortcut_graph).0 <= upper_bound);
                        let new_bounds = data.iter().map(|path| path.bounds_for(&range, shortcut_graph))
                            .fold(Bounds { lower: INFINITY, upper: INFINITY }, |acc, bounds|
                                Bounds { lower: min(acc.lower, bounds.0), upper: min(acc.upper, bounds.1) });
                        if data.len() == 1 {
                            *paths = ShortcutPaths::One(new_bounds, data[0]);
                        } else if data.is_empty() {
                            *paths = ShortcutPaths::None;
                        } else {
                            *bounds = new_bounds;
                        }
                    }
                }
                _ => (),
            }
        }
    }

    pub fn num_path_segments(&self) -> usize {
        self.data.iter().map(ShortcutPaths::num_path_segments).sum()
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_path_segments() > 0 // TODO move into paths
    }

    pub fn unpack(&self, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        for paths in &self.data {
            paths.unpack(shortcut_graph, unpacked_shortcuts, original_edges);
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

        for (i, paths) in self.data.iter().enumerate() {
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
    pub fn bounds(&self) -> (Weight, Weight) {
        match self {
            ShortcutPaths::None => (INFINITY, INFINITY),
            ShortcutPaths::One(Bounds { lower, upper }, _) => (*lower, *upper),
            ShortcutPaths::Multi(Bounds { lower, upper }, _) => (*lower, *upper)
        }
    }

    // TODO for range
    pub fn unpack(&self, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        for source in PathSegmentIter::new(self) {
            source.unpack(shortcut_graph, unpacked_shortcuts, original_edges);
        }
    }

    pub fn num_path_segments(&self) -> usize {
        match self {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_, _) => 1,
            ShortcutPaths::Multi(_, data) => data.len()
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
            ShortcutPaths::One(_, data) => PathSegmentIter::One(once(data)),
            ShortcutPaths::Multi(_, data) => PathSegmentIter::Multi(data.iter())
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
