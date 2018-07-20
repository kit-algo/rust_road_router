use super::*;
use std::slice::Iter as SliceIter;
use std::iter::{once, Once};

use math::RangeExtensions;

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: ShortcutPaths,
    cache: Option<Vec<TTIpp>>
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
            return
        }

        // TODO bounds for range
        let (current_lower_bound, current_upper_bound) = self.bounds(shortcut_graph);
        let (other_lower_bound, other_upper_bound) = other.bounds(shortcut_graph);
        if current_lower_bound >= other_upper_bound {
            self.data = ShortcutPaths::One(other.as_shortcut_data());
            return
        } else if other_lower_bound >= current_upper_bound {
            return
        }

        let range = 0..period();
        let mut data = merge(CooccuringSegIter {
            shortcut_iter: self.non_wrapping_seg_iter(range.clone(), shortcut_graph).peekable(),
            linked_iter: other.non_wrapping_seg_iter(range, shortcut_graph).peekable(),
        }).fold(SegmentAggregator {
            shortcut_path_segments: PathSegmentIter::new(self).peekable(),
            merged_path_segments: Vec::new(),
            merged_atf_segments: Vec::new(),
            linked_shortcut_data: other.as_shortcut_data(),
        }, |mut acc, better_segment| { acc.integrate_segment(better_segment); acc });

        debug_assert!(!data.merged_path_segments.is_empty());
        if data.merged_path_segments.len() > 1 {
            let (_, first_path) = data.merged_path_segments.first().unwrap();
            data.merged_path_segments.push((period(), *first_path));
            self.data = ShortcutPaths::Multi(data.merged_path_segments);
        } else {
            self.data = ShortcutPaths::One(data.merged_path_segments[0].1);
        }
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
        match self.data {
            ShortcutPaths::None => SegmentIterIter::None,
            // TODO maybe split range here
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
        }.flatten()
    }

    pub(super) fn seg_iter<'a, 'b: 'a>(&'b self, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> impl Iterator<Item = MATSeg> + 'a {
        let (first_range, mut second_range) = range.clone().monotonize().split(period());
        second_range.start -= period();
        second_range.end -= period();

        match self.data {
            ShortcutPaths::None => SegmentIterIter::None,
            // TODO maybe split range here
            ShortcutPaths::One(data) => SegmentIterIter::One(
                once(data.non_wrapping_seg_iter(first_range, shortcut_graph))
                    .chain(once(data.non_wrapping_seg_iter(second_range, shortcut_graph)))),
            ShortcutPaths::Multi(ref data) => {
                let (first_index_range, second_index_range) = data.index_ranges(&range, |&(dt, _)| dt);

                let first_iter = data[first_index_range.clone()].windows(2)
                    .map(move |paths| {
                        let (from_at, path) = paths[0];
                        let (to_at, _) = paths[1];
                        path.non_wrapping_seg_iter((from_at..to_at).intersection(&first_range), shortcut_graph)
                    });
                let second_iter = data[second_index_range.clone()].windows(2)
                    .map(move |paths| {
                        let (from_at, path) = paths[0];
                        let (to_at, _) = paths[1];
                        path.non_wrapping_seg_iter((from_at..to_at).intersection(&second_range), shortcut_graph)
                    });
                SegmentIterIter::Multi(first_iter.chain(second_iter))
            }
        }.flatten()
    }

    pub fn bounds(&self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        if !self.is_valid_path() {
            return (INFINITY, INFINITY);
        }

        if let Some(ref ipps) = self.cache {
            if !ipps.is_empty() {
                return ipps.iter().fold((INFINITY, 0), |(acc_min, acc_max), ipp| (min(acc_min, ipp.val), max(acc_max, ipp.val)))
            }
        }

        PathSegmentIter::new(self)
            .map(|(_, source)| source.bounds(shortcut_graph))
            .fold((INFINITY, 0), |(acc_min, acc_max), (seg_min, seg_max)| (min(acc_min, seg_min), max(acc_max, seg_max)))
    }

    pub fn num_segments(&self) -> usize {
        match self.data {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_) => 1,
            ShortcutPaths::Multi(ref data) => data.len()
        }
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_segments() > 0
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

    fn segment_range(&self, segment: usize) -> Range<Timestamp> {
        match self.data {
            ShortcutPaths::None => panic!("there are no segment ranges on an empty shortcut"),
            ShortcutPaths::One(_) => {
                debug_assert_eq!(segment, 0);
                Range { start: 0, end: 0 }
            },
            ShortcutPaths::Multi(ref data) => {
                let next_index = (segment + 1) % data.len();
                Range { start: data[segment].0, end: data[next_index].0 }
            }
        }
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
struct SegmentAggregator<'a> {
    shortcut_path_segments: Peekable<PathSegmentIter<'a>>,
    merged_path_segments: Vec<(Timestamp, ShortcutData)>,
    merged_atf_segments: Vec<MATSeg>,
    linked_shortcut_data: ShortcutData
}

impl<'a> SegmentAggregator<'a> {
    fn integrate_segment(&mut self, segment: BetterSegment) {
        match self.merged_atf_segments.last_mut() {
            Some(prev_segment) => {
                match segment {
                    BetterSegment::Shortcut(segment) => {
                        if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                            let mut prev = if self.merged_path_segments.len() > 1 {
                                Some(self.merged_path_segments[self.merged_path_segments.len() - 2].1)
                            } else {
                                None
                            };
                            while self.shortcut_path_segments.peek().unwrap().0 < segment.valid.start {
                                prev = Some(*self.shortcut_path_segments.peek().unwrap().1);
                                self.shortcut_path_segments.next();
                            }

                            self.merged_path_segments.push((segment.valid.start, prev.unwrap()));
                        }

                        while let Some(next) = self.shortcut_path_segments.peek() {
                            if next.0 < segment.valid.end {
                                self.merged_path_segments.push((next.0, *next.1));
                                self.shortcut_path_segments.next();
                            } else {
                                break;
                            }
                        }

                        if !prev_segment.combine(&segment) {
                            self.merged_atf_segments.push(segment);
                        }
                    },
                    BetterSegment::Linked(segment) => {
                        if self.merged_path_segments.last().unwrap().1 != self.linked_shortcut_data {
                            self.merged_path_segments.push((segment.valid.start, self.linked_shortcut_data));
                        }
                        if !prev_segment.combine(&segment) {
                            self.merged_atf_segments.push(segment);
                        }
                    },
                    BetterSegment::Equal(shortcut_seg, linked_seg) => {
                        if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                            if !prev_segment.combine(&linked_seg) {
                                self.merged_atf_segments.push(linked_seg);
                            }
                        } else {
                            if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                                let mut prev = if self.merged_path_segments.len() > 1 {
                                    Some(self.merged_path_segments[self.merged_path_segments.len() - 2].1)
                                } else {
                                    None
                                };
                                while self.shortcut_path_segments.peek().unwrap().0 < shortcut_seg.valid.start {
                                    prev = Some(*self.shortcut_path_segments.peek().unwrap().1);
                                    self.shortcut_path_segments.next();
                                }

                                self.merged_path_segments.push((shortcut_seg.valid.start, prev.unwrap()));
                            }

                            while let Some(next) = self.shortcut_path_segments.peek() {
                                if next.0 < shortcut_seg.valid.end {
                                    self.merged_path_segments.push((next.0, *next.1));
                                    self.shortcut_path_segments.next();
                                } else {
                                    break;
                                }
                            }

                            if !prev_segment.combine(&shortcut_seg) {
                                self.merged_atf_segments.push(shortcut_seg);
                            }
                        }
                    },
                }
            },
            None => {
                match segment {
                    BetterSegment::Shortcut(segment) => {
                        self.merged_atf_segments.push(segment);
                        self.merged_path_segments.push(self.shortcut_path_segments.next().map(|(at, data)| (at, *data)).unwrap());
                    },
                    BetterSegment::Linked(segment) => {
                        self.merged_atf_segments.push(segment);
                        self.merged_path_segments.push((0, self.linked_shortcut_data));
                    },
                    BetterSegment::Equal(shortcut_seg, _linked_seg) => {
                        self.merged_atf_segments.push(shortcut_seg);
                        self.merged_path_segments.push(self.shortcut_path_segments.next().map(|(at, data)| (at, *data)).unwrap());
                    },
                }
            },
        }
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

struct CooccuringSegIter<SegmentIter: Iterator<Item = MATSeg>, LinkedSegIter: Iterator<Item = MATSeg>> {
    shortcut_iter: Peekable<SegmentIter>,
    linked_iter: Peekable<LinkedSegIter>
}

impl<'a, SegmentIter: Iterator<Item = MATSeg>, LinkedSegIter: Iterator<Item = MATSeg>> Iterator for CooccuringSegIter<SegmentIter, LinkedSegIter> {
    type Item = (MATSeg, MATSeg);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.shortcut_iter.peek().cloned(), self.linked_iter.peek().cloned()) {
            (Some(shortcut_next_seg), Some(linked_next_seg)) => {
                if shortcut_next_seg.valid.end == linked_next_seg.valid.end {
                    self.shortcut_iter.next();
                    self.linked_iter.next();
                } else if shortcut_next_seg.valid.end <= linked_next_seg.valid.end {
                    self.shortcut_iter.next();
                } else {
                    self.linked_iter.next();
                }
                Some((shortcut_next_seg, linked_next_seg))
            },
            (None, None) => None,
            _ => panic!("broken valid ranges in parallel iteration")
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
        // run_test_with_periodicity(8, || {
        //     let graph = TDGraph::new(
        //         vec![0, 1,    3, 3],
        //         vec![2, 0, 2],
        //         vec![0,  1,     3,     5],
        //         vec![0,  2, 6,  2, 6],
        //         vec![1,  1, 5,  6, 2],
        //     );

        //     let cch_first_out = vec![0, 1, 3, 3];
        //     let cch_head =      vec![2, 0, 2];

        //     let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(Some(2))];
        //     let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

        //     let mut shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        //     let all_ipps: Vec<_> = shortcut_graph.get_upward(2).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
        //     assert_eq!(all_ipps, vec![(2,6), (6,2)]);
        //     let all_ipps: Vec<_> = Linked::new(1, 0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
        //     assert_eq!(all_ipps, vec![(2,2), (6,6)]);

        //     shortcut_graph.merge_upward(2, Linked::new(1, 0));

        //     assert_eq!(shortcut_graph.get_upward(2).data, ShortcutPaths::Multi(vec![(0, ShortcutData::new(ShortcutSource::Shortcut(1, 0))), (4, ShortcutData::new(ShortcutSource::OriginalEdge(2)))]));

        //     let all_merged_ipps: Vec<_> = shortcut_graph.get_upward(2).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
        //     assert_eq!(all_merged_ipps, vec![(0,4), (2,2), (4,4), (6,2)]);
        // });
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
