use std::iter::Once;
use std::slice::Iter as SliceIter;
use super::*;
use rank_select_map::BitVec;

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: ShortcutPaths,
    lower_bound: Weight,
    upper_bound: Weight,
}

#[derive(Debug, Clone, PartialEq)]
enum ShortcutPaths {
    None,
    One(ShortcutData),
    Multi(Vec<ShortcutData>)
}

impl Shortcut {
    pub fn new(source: Option<(EdgeId, PiecewiseLinearFunction)>) -> Shortcut {
        match source {
            Some((edge, plf)) => {
                let data = ShortcutPaths::One(ShortcutData::new(ShortcutSource::OriginalEdge(edge)));
                let (lower_bound, upper_bound) = plf.bounds();
                Shortcut { data, lower_bound, upper_bound }
            },
            None => Shortcut { data: ShortcutPaths::None, lower_bound: INFINITY, upper_bound: INFINITY },
        }
    }

    pub fn merge(&mut self, other: Linked, shortcut_graph: &ShortcutGraph) {
        if !other.is_valid_path(shortcut_graph) {
            return
        }
        let (other_lower_bound, other_upper_bound) = other.bounds(shortcut_graph);

        if !self.is_valid_path() {
            self.data = ShortcutPaths::One(other.as_shortcut_data());
            self.lower_bound = other_lower_bound;
            self.upper_bound = other_upper_bound;
            return
        }

        let (current_lower_bound, current_upper_bound) = self.bounds();
        if current_lower_bound >= other_upper_bound {
            self.data = ShortcutPaths::One(other.as_shortcut_data());
            self.lower_bound = other_lower_bound;
            self.upper_bound = other_upper_bound;
            return
        } else if other_lower_bound >= current_upper_bound {
            return
        }

        debug_assert!(current_lower_bound < INFINITY);
        debug_assert!(current_upper_bound < INFINITY);
        debug_assert!(other_lower_bound < INFINITY);
        debug_assert!(other_upper_bound < INFINITY);
        self.lower_bound = min(current_lower_bound, other_lower_bound);
        self.upper_bound = min(current_upper_bound, other_upper_bound);
        match self.data {
            ShortcutPaths::None => self.data = ShortcutPaths::One(other.as_shortcut_data()),
            ShortcutPaths::One(data) => self.data = ShortcutPaths::Multi(vec![data, other.as_shortcut_data()]),
            ShortcutPaths::Multi(ref mut data) => {
                data.push(other.as_shortcut_data())
            }
        }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match self.data {
            ShortcutPaths::None => INFINITY,
            ShortcutPaths::One(data) => data.evaluate(departure, shortcut_graph),
            ShortcutPaths::Multi(ref data) => {
                data.iter().map(|path| path.evaluate(departure, shortcut_graph)).min().unwrap()
            }
        }
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        debug_assert!(self.lower_bound < INFINITY || !self.is_valid_path());
        debug_assert!(self.upper_bound < INFINITY || !self.is_valid_path());
        (self.lower_bound, self.upper_bound)
    }

    pub fn remove_dominated(&mut self, shortcut_graph: &ShortcutGraph) {
        self.remove_dominated_by(shortcut_graph, self.upper_bound)
    }

    pub fn remove_dominated_by(&mut self, shortcut_graph: &ShortcutGraph, upper_bound: Weight) {
        match self.data {
            ShortcutPaths::One(ref mut data) => {
                if data.bounds(shortcut_graph).0 > upper_bound {
                    self.lower_bound = INFINITY;
                    self.upper_bound = INFINITY;
                    self.data = ShortcutPaths::None;
                }
            },
            ShortcutPaths::Multi(ref mut data) => {
                data.retain(|path| path.bounds(shortcut_graph).0 <= upper_bound);
                if data.len() == 1 {
                    self.data = ShortcutPaths::One(data[0]);
                } else if data.is_empty() {
                    self.upper_bound = INFINITY;
                    self.lower_bound = INFINITY;
                    self.data = ShortcutPaths::None;
                }
            }
            _ => (),
        }
    }

    pub fn num_path_segments(&self) -> usize {
        match self.data {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_) => 1,
            ShortcutPaths::Multi(ref data) => data.len()
        }
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_path_segments() > 0
    }

    pub fn unpack(&self, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        for source in PathSegmentIter::new(self) {
            source.unpack(shortcut_graph, unpacked_shortcuts, original_edges);
        }
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        println!("{:?}", self.data);
        let mut s = String::from("Shortcut: ");
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

    pub fn validate_does_not_contain(&self, edge_id: EdgeId, shortcut_graph: &ShortcutGraph) {
        for source in PathSegmentIter::new(self) {
            source.validate_does_not_contain(edge_id, shortcut_graph);
        }
    }
}

#[derive(Debug)]
enum PathSegmentIter<'a> {
    None,
    One(Once<&'a ShortcutData>),
    Multi(SliceIter<'a, ShortcutData>)
}

impl<'a> PathSegmentIter<'a> {
    fn new(shortcut: &'a Shortcut) -> PathSegmentIter<'a> {
        match shortcut.data {
            ShortcutPaths::None => PathSegmentIter::None,
            ShortcutPaths::One(ref data) => PathSegmentIter::One(once(data)),
            ShortcutPaths::Multi(ref data) => PathSegmentIter::Multi(data.iter())
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
