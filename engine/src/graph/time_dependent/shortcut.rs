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
                // TODO maybe clean up
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
        let upper_bound = self.upper_bound;
        if let ShortcutPaths::Multi(data) = &mut self.data {
            data.retain(|path| path.bounds(shortcut_graph).0 <= upper_bound);
            debug_assert!(!data.is_empty());
            if data.len() == 1 {
                self.data = ShortcutPaths::One(data[0]);
            }
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

#[derive(Debug)]
enum SegmentIterIter<InnerIter: Iterator<Item = MATSeg>, OneSegIter: Iterator<Item = InnerIter>, MultiSegIter: Iterator<Item = InnerIter>> {
    None,
    One(OneSegIter),
    Multi(MultiSegIter)
}

impl<'a, InnerIter, OneSegIter, MultiSegIter> Iterator for SegmentIterIter<InnerIter, OneSegIter, MultiSegIter> where
    InnerIter: Iterator<Item = MATSeg>,
    OneSegIter: Iterator<Item = InnerIter>,
    MultiSegIter: Iterator<Item = InnerIter>,
{
    type Item = InnerIter;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SegmentIterIter::None => None,
            SegmentIterIter::One(iter) => iter.next(),
            SegmentIterIter::Multi(iter) => iter.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merging() {
        run_test_with_periodicity(8, || {
            let graph = TDGraph::new(
                vec![0, 1,    3, 3],
                vec![2, 0, 2],
                vec![0,   2,       6,      10],
                vec![0,8, 0,2,6,8, 0,2,6,8],
                vec![1,1, 3,1,5,3, 4,6,2,4],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(Some(2))];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let mut shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            shortcut_graph.merge_upward(2, Linked::new(1, 0));

            assert_eq!(shortcut_graph.get_upward(2).data,
                ShortcutPaths::Multi(vec![
                    (0, ShortcutData::new(ShortcutSource::Shortcut(1, 0))),
                    (4, ShortcutData::new(ShortcutSource::OriginalEdge(2))),
                    (8, ShortcutData::new(ShortcutSource::Shortcut(1, 0)))]));
        });
    }

    #[test]
    fn test_eval() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 4, 8],
                vec![0, 1, 3, 10,  0, 5, 8, 10],
                vec![2, 2, 5, 2,   1, 2, 1, 1],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            assert_eq!(shortcut_graph.get_downward(1).evaluate(0, &shortcut_graph), 1);
            assert_eq!(shortcut_graph.get_upward(0).evaluate(1, &shortcut_graph), 2);
        });
    }
}
