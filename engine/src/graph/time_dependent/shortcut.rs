use super::*;
use std::slice::Iter as SliceIter;
use std::iter::Once;

use math::RangeExtensions;
use ::sorted_search_slice_ext::*;

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: ShortcutPaths,
    cache: Option<Vec<MATSeg>>
}

#[derive(Debug, Clone, PartialEq)]
enum ShortcutPaths {
    None,
    One(ShortcutData),
    Multi(Vec<(Timestamp, ShortcutData)>) // TODO wasting space?
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>) -> Shortcut {
        let data = match source {
            Some(edge) => ShortcutPaths::One(ShortcutData::new(ShortcutSource::OriginalEdge(edge))),
            None => ShortcutPaths::None,
        };
        Shortcut { data, cache: None }
    }

    pub fn merge(&mut self, other: Linked, shortcut_graph: &ShortcutGraph) {
        if !other.is_valid_path(shortcut_graph) {
            return
        }
        if !self.is_valid_path() {
            self.data = ShortcutPaths::One(other.as_shortcut_data());
            self.cache = Some(other.non_wrapping_seg_iter(0..period(), shortcut_graph).collect());
            return
        }

        // TODO bounds for range
        let (current_lower_bound, current_upper_bound) = self.bounds(shortcut_graph);
        let (other_lower_bound, other_upper_bound) = other.bounds(shortcut_graph);
        if current_lower_bound >= other_upper_bound {
            self.cache = Some(other.non_wrapping_seg_iter(0..period(), shortcut_graph).collect());
            self.data = ShortcutPaths::One(other.as_shortcut_data());
            return
        } else if other_lower_bound >= current_upper_bound {
            return
        }

        let range = 0..period();
        let data = merge(
            self.non_wrapping_seg_iter(range.clone(), shortcut_graph),
            other.non_wrapping_seg_iter(range, shortcut_graph)
        ).fold(SegmentAggregator::new(
            other.as_shortcut_data(), PathSegmentIter::new(self)),
            |mut acc, better_segment| { acc.integrate_segment(better_segment); acc });

        let (mut merged_path_segments, cache) = data.decompose();
        debug_assert!(!merged_path_segments.is_empty());
        if merged_path_segments.len() > 1 {
            let (_, first_path) = merged_path_segments.first().unwrap();
            merged_path_segments.push((period(), *first_path));
            self.data = ShortcutPaths::Multi(merged_path_segments);
        } else {
            self.data = ShortcutPaths::One(merged_path_segments[0].1);
        }
        // debug_assert_eq!(cache, self.non_wrapping_seg_iter(0..period(), shortcut_graph).collect::<Vec<_>>());
        self.cache = Some(cache);
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match self.data {
            ShortcutPaths::None => INFINITY,
            ShortcutPaths::One(data) => data.evaluate(departure, shortcut_graph),
            ShortcutPaths::Multi(ref data) => {
                match data.sorted_search_by_key(&departure, |&(time, _)| time) {
                    Ok(index) => data[index].1.evaluate(departure, shortcut_graph),
                    Err(index) => {
                        let index = (index + data.len() - 1) % data.len();
                        data[index].1.evaluate(departure, shortcut_graph)
                    },
                }
            }
        }
    }

    pub(super) fn non_wrapping_seg_iter<'a, 'b: 'a>(&'b self, range: Range<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> impl Iterator<Item = MATSeg> + 'a {
        if let Some(cache) = &self.cache {
            return TwoTypeIter::First(cache.iter().filter({ let range = range.clone(); move |seg| {
                !seg.valid.is_intersection_empty(&range)
            }}).map(move |seg| {
                let mut seg = seg.clone();
                seg.valid = seg.valid.intersection(&range);
                seg
            }))
        }

        if self.contains_non_trivial_data() {
            println!("cache miss!");
        }

        let iter = match self.data {
            ShortcutPaths::None => SegmentIterIter::None,
            ShortcutPaths::One(data) => SegmentIterIter::One(once(data.non_wrapping_seg_iter(range, shortcut_graph))),
            ShortcutPaths::Multi(ref data) => {
                let index_range = data.index_range(&range, |&(dt, _)| dt);

                SegmentIterIter::Multi(data[index_range.clone()].windows(2)
                    .map(move |paths| {
                        let (from_at, path) = paths[0];
                        let (to_at, _) = paths[1];
                        path.non_wrapping_seg_iter((from_at..to_at).intersection(&range), shortcut_graph)
                    }))
            }
        }.flatten();
        TwoTypeIter::Second(iter)
    }

    fn contains_non_trivial_data(&self) -> bool {
        match self.data {
            ShortcutPaths::None => false,
            ShortcutPaths::One(data) => {
                match data.source() {
                    ShortcutSource::OriginalEdge(_) => false,
                    ShortcutSource::Shortcut(_, _) => true,
                }
            },
            ShortcutPaths::Multi(ref data) => {
                data.len() > 1 || match data[0].1.source() {
                    ShortcutSource::OriginalEdge(_) => false,
                    ShortcutSource::Shortcut(_, _) => true,
                }
            }
        }
    }

    pub fn bounds(&self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        if !self.is_valid_path() {
            return (INFINITY, INFINITY);
        }

        // TODO use cache

        PathSegmentIter::new(self)
            .map(|(_, source)| source.bounds(shortcut_graph))
            .fold((INFINITY, 0), |(acc_min, acc_max), (seg_min, seg_max)| (min(acc_min, seg_min), max(acc_max, seg_max)))
    }

    pub fn num_path_segments(&self) -> usize {
        match self.data {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_) => 1,
            ShortcutPaths::Multi(ref data) => data.len() - 1
        }
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_path_segments() > 0
    }

    pub fn cache_ipps(&mut self, _shortcut_graph: &ShortcutGraph) {
        // if self.is_valid_path() {
        //     let range = WrappingRange::new(Range { start: 0, end: 0 });
        //     let ipps = self.ipp_iter(range, shortcut_graph).collect();
        //     self.cache = Some(ipps);
        // }
    }

    pub fn clear_cache(&mut self) {
        self.cache = None
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        let mut s = String::from("Shortcut: ");
        for (time, source) in PathSegmentIter::new(self) {
            s.push('\n');
            for _ in 0..indent {
                s.push(' ');
                s.push(' ');
            }
            s = s + &format!("{}: ", time) + &source.debug_to_s(shortcut_graph, indent + 1);
        }
        s
    }
}

#[derive(Debug)]
enum PathSegmentIter<'a> {
    None,
    One(Once<&'a ShortcutData>),
    Multi(SliceIter<'a, (Timestamp, ShortcutData)>)
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
    type Item = (Timestamp, &'a ShortcutData);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PathSegmentIter::None => None,
            PathSegmentIter::One(iter) => iter.next().map(|reference| (0, reference)),
            PathSegmentIter::Multi(iter) => iter.next().map(|&(time, ref source)| (time, source))
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
