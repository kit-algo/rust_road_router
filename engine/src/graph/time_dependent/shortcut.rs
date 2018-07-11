use super::*;
use std::slice::Iter as SliceIter;
use std::iter::{once, Once};
// use benchmark::measure;

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: ShortcutPaths,
    cache: Option<Vec<TTIpp>>
}

#[derive(Debug, Clone, PartialEq)]
enum ShortcutPaths {
    None,
    One(ShortcutData),
    Multi(Vec<(Timestamp, ShortcutData)>)
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

        let mut ipp_counter = 0;

        let mut intersections =
            // measure("combined ipp iteration and intersecting", ||
        {
            let range = WrappingRange::new(Range { start: 0, end: 0 });
            let ipp_iter = MergingIter {
                shortcut_iter: self.ipp_iter(range.clone(), shortcut_graph).peekable(),
                linked_iter: other.ipp_iter(range, shortcut_graph).peekable()
            }.inspect(|&(at, value)| {
                match value {
                    IppSource::First(value) => {
                        debug_assert!(abs_diff(value, self.evaluate(at, shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", at, value, self.evaluate(at, shortcut_graph), self.debug_to_s(shortcut_graph, 0));
                    },
                    IppSource::Second(value) => {
                        debug_assert!(abs_diff(value, other.evaluate(at, shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", at, value, other.evaluate(at, shortcut_graph), other.debug_to_s(shortcut_graph, 0));
                    },
                    IppSource::Both(first_value, second_value) => {
                        debug_assert!(abs_diff(first_value, self.evaluate(at, shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", at, first_value, self.evaluate(at, shortcut_graph), self.debug_to_s(shortcut_graph, 0));
                        debug_assert!(abs_diff(second_value, other.evaluate(at, shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", at, second_value, other.evaluate(at, shortcut_graph), other.debug_to_s(shortcut_graph, 0));
                    }
                }
                ipp_counter += 1;
            });

            intersections(ipp_iter, || self.evaluate(0, shortcut_graph), || other.evaluate(0, shortcut_graph), period())
        }
        // )
        ;

        // println!("iterated ipps: {:?}", ipp_counter);

        if intersections.is_empty() {
            if self.evaluate(0, shortcut_graph) <= other.evaluate(0, shortcut_graph) {
                return
            } else {
                self.data = ShortcutPaths::One(other.as_shortcut_data());
                return
            }
        }

        let c = intersections.len();
        {
            let collect = || -> Vec<(Timestamp, IppSource)> {
                MergingIter {
                    shortcut_iter: self.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), shortcut_graph).peekable(),
                    linked_iter: other.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), shortcut_graph).peekable()
                }.collect()
            };
            debug_assert_eq!(c % 2, 0, "intersections: {:?}, ipps: {:?}", intersections, collect());
        }

        for &(intersection, is_self_better) in &intersections {
            let before_intersection = if intersection < 1 { intersection + period() - 1 } else { intersection - 1 };
            let debug_output = || {
                // let collected: Vec<(Timestamp, IppSource)> = MergingIter {
                //     shortcut_iter: self.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.period()), shortcut_graph).peekable(),
                //     linked_iter: other.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.period()), shortcut_graph).peekable()
                // }.collect();
                // format!("intersection: {} {} {}, before intersection: {} {} {}, {} \n collected: {:?}",
                format!("intersection: {} {} {}, before intersection: {} {} {}",
                    intersection, self.evaluate(intersection, shortcut_graph), other.evaluate(intersection, shortcut_graph),
                    before_intersection, self.evaluate(before_intersection, shortcut_graph), other.evaluate(before_intersection, shortcut_graph),
                    // self.debug_to_s(shortcut_graph, 0), collected
                    )
            };

            if is_self_better {
                debug_assert!(self.evaluate(intersection, shortcut_graph) <= other.evaluate(intersection, shortcut_graph), "{}", debug_output());
                // debug_assert!(self.evaluate(before_intersection, shortcut_graph) >= other.evaluate(before_intersection, shortcut_graph), "{}", debug_output());
            } else {
                debug_assert!(self.evaluate(intersection, shortcut_graph) >= other.evaluate(intersection, shortcut_graph), "{}", debug_output());
                // debug_assert!(self.evaluate(before_intersection, shortcut_graph) <= other.evaluate(before_intersection, shortcut_graph), "{}", debug_output());
            }
        }


        if intersections[c - 2].0 > intersections[c - 1].0 {
            let last = intersections[c - 1];
            let second_last = intersections[c - 2];
            intersections.insert(0, last);
            intersections.insert(0, second_last);
        } else {
            let first = intersections[0];
            let last = intersections[c - 1];
            intersections.insert(0, last);
            intersections.push(first);
        }
        intersections.last_mut().unwrap().0 += period();

        let mut new_shortcut_data = vec![];

        let mut intersections = intersections.into_iter().peekable();
        let (_, mut is_self_currently_better) = intersections.next().unwrap();
        {
            let mut prev_source = self.last_segment_source().unwrap();

            for (time, source) in PathSegmentIter::new(self) {
                // println!("time: {}, source: {:?}", time, source);
                while time >= intersections.peek().unwrap().0 {
                    let (peeked_ipp, self_better_at_peeked) = intersections.next().unwrap();
                    // println!("peeked {} {}", peeked_ipp, self_better_at_peeked);

                    if self_better_at_peeked {
                        // println!("self better");
                        if time > peeked_ipp {
                            // println!("pushing prev source {:?}", prev_source);
                            new_shortcut_data.push((peeked_ipp, *prev_source));
                        }
                    } else {
                        // println!("other better - pushing other {:?}", other.as_shortcut_data());
                        new_shortcut_data.push((peeked_ipp, other.as_shortcut_data()));
                    }

                    is_self_currently_better = self_better_at_peeked;
                }

                if is_self_currently_better {
                    // println!("applying current source: {:?}", *source);
                    new_shortcut_data.push((time, *source));
                }

                prev_source = source;
            }

            for (intersection_ipp, segment_self_better) in intersections {
                if segment_self_better {
                    // println!("adding rest self {} {:?}", intersection_ipp, *prev_source);
                    new_shortcut_data.push((intersection_ipp, *prev_source));
                } else {
                    // println!("adding rest other {} {:?}", intersection_ipp, other.as_shortcut_data());
                    new_shortcut_data.push((intersection_ipp, other.as_shortcut_data()));
                }
            }
        }
        new_shortcut_data.pop();

        for values in new_shortcut_data.windows(2) {
            debug_assert!(values[0].0 < values[1].0);
        }

        debug_assert!(new_shortcut_data.len() > 1);
        self.data = ShortcutPaths::Multi(new_shortcut_data); // TODO can be one???
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

    pub fn ipp_iter<'a, 'b: 'a>(&'b self, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        Iter::new(&self, range, shortcut_graph)
    }

    pub(super) fn seg_iter<'a, 'b: 'a>(&'b self, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> SegmentIter<'a, 'b> {
        SegmentIter::new(&self, range, shortcut_graph)
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

    pub fn cache_ipps(&mut self, shortcut_graph: &ShortcutGraph) {
        if self.is_valid_path() {
            let range = WrappingRange::new(Range { start: 0, end: 0 });
            let ipps = self.ipp_iter(range, shortcut_graph).collect();
            self.cache = Some(ipps);
        }
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

    fn last_segment_source(&self) -> Option<&ShortcutData> {
        match self.data {
            ShortcutPaths::None => None,
            ShortcutPaths::One(ref data) => Some(data),
            ShortcutPaths::Multi(ref data) => data.last().map(|&(_, ref data)| data)
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

struct MergingIter<'a, 'b: 'a> {
    shortcut_iter: Peekable<Iter<'a, 'b>>,
    linked_iter: Peekable<linked::Iter<'a>>
}

impl<'a, 'b> Iterator for MergingIter<'a, 'b> {
    type Item = (Timestamp, IppSource);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.shortcut_iter.peek().cloned(), self.linked_iter.peek().cloned()) {
            (Some(self_next_ipp), Some(other_next_ipp)) => {
                if self_next_ipp.at == other_next_ipp.at {
                    self.shortcut_iter.next();
                    self.linked_iter.next();
                    Some((self_next_ipp.at, IppSource::Both(self_next_ipp.val, other_next_ipp.val)))
                } else if self_next_ipp.at <= other_next_ipp.at {
                    self.shortcut_iter.next();
                    Some((self_next_ipp.at, IppSource::First(self_next_ipp.val)))
                } else {
                    self.linked_iter.next();
                    Some((other_next_ipp.at, IppSource::Second(other_next_ipp.val)))
                }
            },
            (None, Some(other_next_ipp)) => {
                self.linked_iter.next();
                Some((other_next_ipp.at, IppSource::Second(other_next_ipp.val)))
            },
            (Some(self_next_ipp), None) => {
                self.shortcut_iter.next();
                Some((self_next_ipp.at, IppSource::First(self_next_ipp.val)))
            },
            (None, None) => None
        }
    }
}

struct CooccuringSegIter<'a, 'b: 'a> {
    shortcut_iter: Peekable<SegmentIter<'a, 'b>>,
    linked_iter: Peekable<linked::SegmentIter<'a>>
}

impl<'a, 'b> Iterator for CooccuringSegIter<'a, 'b> {
    type Item = (TTFSeg, TTFSeg);

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

pub struct Iter<'a, 'b: 'a> {
    shortcut: &'b Shortcut,
    shortcut_graph: &'a ShortcutGraph<'a>,
    range: WrappingRange,
    current_index: usize,
    segment_iter_state: InitialPathSegmentIterState,
    current_source_iter: Option<Box<Iterator<Item = TTIpp> + 'a>>,
    cached_iter: Option<WrappingSliceIter<'b>>,
}

impl<'a, 'b> Iter<'a, 'b> {
    fn new(shortcut: &'b Shortcut, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        if let Some(ref cache) = shortcut.cache {
            let cached_iter = WrappingSliceIter::new(cache, range.clone());
            return Iter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialPathSegmentIterState::Finishing, current_source_iter: None, cached_iter: Some(cached_iter) }
        }

        match shortcut.data {
            ShortcutPaths::None => Iter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialPathSegmentIterState::Finishing, current_source_iter: None, cached_iter: None },
            ShortcutPaths::One(data) => {
                let mut segment_range = WrappingRange::new(shortcut.segment_range(0));
                segment_range.shift_start(range.start());

                Iter {
                    shortcut, shortcut_graph, range, current_index: 0, cached_iter: None,
                    segment_iter_state: InitialPathSegmentIterState::InitialCompleted(0),
                    current_source_iter: Some(Box::new(data.ipp_iter(segment_range, shortcut_graph)) as Box<Iterator<Item = TTIpp>>)
                }
            },
            ShortcutPaths::Multi(ref data) => {
                let (current_index, segment_iter_state, current_source_iter) = match data.sorted_search_by_key(&range.start(), |&(time, _)| time) {
                    Ok(index) => (index, InitialPathSegmentIterState::InitialCompleted(index), None),
                    Err(index) => {
                        let current_index = (index + data.len() - 1) % data.len();
                        let mut segment_range = WrappingRange::new(shortcut.segment_range(current_index));
                        let segment_iter_state = InitialPathSegmentIterState::InitialPartialyCompleted(current_index);
                        segment_range.shift_start(range.start());
                        // println!("shortcut segment iter range {:?}", segment_range);
                        (current_index, segment_iter_state, Some(Box::new(data[current_index].1.ipp_iter(segment_range, shortcut_graph)) as Box<Iterator<Item = TTIpp>>))
                    },
                };

                Iter { shortcut, shortcut_graph, range, current_index, segment_iter_state, current_source_iter, cached_iter: None }
            }
        }
        // println!("new shortcut iter range {:?}, index: {}, iter created? {}", range, current_index, current_source_iter.is_some());
    }

    fn calc_next(&mut self) -> Option<<Self as Iterator>::Item> {
        // println!("shortcut next");
        match self.current_source_iter {
            Some(ref mut current_source_iter) => {
                match current_source_iter.next() {
                    Some(ipp) => {
                        debug_assert!(abs_diff(ipp.val, self.shortcut.evaluate(ipp.at, self.shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", ipp.at, ipp.val, self.shortcut.evaluate(ipp.at, self.shortcut_graph), self.shortcut.debug_to_s(self.shortcut_graph, 0));
                        if self.range.contains(ipp.at) {
                            // println!("shortcut result {}", ipp);
                            Some(ipp)
                        } else {
                            None
                        }
                    },
                    None => {
                        self.current_source_iter = None;
                        match self.segment_iter_state {
                            InitialPathSegmentIterState::InitialCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.num_segments();
                                if self.current_index != initial_index {
                                    self.next()
                                } else {
                                    None
                                }
                            },
                            InitialPathSegmentIterState::InitialPartialyCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.num_segments();
                                if self.current_index == initial_index {
                                    self.segment_iter_state = InitialPathSegmentIterState::Finishing;
                                    self.next()
                                } else {
                                    self.next()
                                }
                            },
                            InitialPathSegmentIterState::Finishing => None,
                        }
                    },
                }
            },
            None => {
                match self.shortcut.data {
                    ShortcutPaths::None => None,
                    ShortcutPaths::One(data) => {
                        if self.range.contains(0) {
                            let mut segment_range = self.shortcut.segment_range(self.current_index);
                            if self.segment_iter_state == InitialPathSegmentIterState::Finishing {
                                segment_range.end = self.range.end()
                            }
                            let segment_range = WrappingRange::new(segment_range);
                            self.current_source_iter = Some(Box::new(data.ipp_iter(segment_range, self.shortcut_graph)));
                            self.next()
                        } else {
                            None
                        }
                    },
                    ShortcutPaths::Multi(ref data) => {
                        let ipp = data[self.current_index].0;

                        if self.range.contains(ipp) {
                            let mut segment_range = self.shortcut.segment_range(self.current_index);
                            segment_range.start = (segment_range.start + 1) % period();
                            if self.segment_iter_state == InitialPathSegmentIterState::Finishing {
                                segment_range.end = self.range.end()
                            }
                            let segment_range = WrappingRange::new(segment_range);
                            self.current_source_iter = Some(Box::new(data[self.current_index].1.ipp_iter(segment_range, self.shortcut_graph)));
                            Some(TTIpp::new(ipp, data[self.current_index].1.evaluate(ipp, self.shortcut_graph))) // TODO optimize?
                        } else {
                            None
                        }
                    }
                }
            },
        }
    }
}

impl<'a, 'b> Iterator for Iter<'a, 'b> {
    type Item = TTIpp;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.cached_iter {
            iter.next().cloned()
        } else {
            self.calc_next()
        }
    }
}

pub(super) struct SegmentIter<'a, 'b: 'a> {
    shortcut: &'b Shortcut,
    shortcut_graph: &'a ShortcutGraph<'a>,
    range: WrappingRange,
    current_index: usize,
    segment_iter_state: InitialPathSegmentIterState,
    current_source_iter: Option<Box<Iterator<Item = TTFSeg> + 'a>>,
    source_contained_segments: bool,
    cached_iter: Option<WrappingSliceIter<'b>>,
}

impl<'a, 'b> SegmentIter<'a, 'b> {
    fn new(shortcut: &'b Shortcut, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> SegmentIter<'a, 'b> {
        // if let Some(ref cache) = shortcut.cache {
        //     let cached_iter = WrappingSliceIter::new(cache, range.clone());
        //     return SegmentIter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialPathSegmentIterState::Finishing, current_source_iter: None, cached_iter: Some(cached_iter) }
        // }

        match shortcut.data {
            ShortcutPaths::None => SegmentIter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialPathSegmentIterState::Finishing, current_source_iter: None, source_contained_segments: false, cached_iter: None },
            ShortcutPaths::One(data) => {
                SegmentIter {
                    shortcut, shortcut_graph, range: range.clone(), current_index: 0, source_contained_segments: false, cached_iter: None,
                    segment_iter_state: InitialPathSegmentIterState::InitialCompleted(0),
                    current_source_iter: Some(Box::new(data.seg_iter(range, shortcut_graph)) as Box<Iterator<Item = TTFSeg>>)
                }
            },
            ShortcutPaths::Multi(ref data) => {
                let (current_index, segment_iter_state, current_source_iter) = match data.sorted_search_by_key(&range.start(), |&(time, _)| time) {
                    Ok(index) => (index, InitialPathSegmentIterState::InitialCompleted(index), None),
                    Err(index) => {
                        let current_index = (index + data.len() - 1) % data.len();
                        let mut segment_range = WrappingRange::new(shortcut.segment_range(current_index));
                        let segment_iter_state = InitialPathSegmentIterState::InitialPartialyCompleted(current_index);
                        segment_range.shift_start(range.start());
                        // println!("shortcut segment iter range {:?}", segment_range);
                        (current_index, segment_iter_state, Some(Box::new(data[current_index].1.seg_iter(segment_range, shortcut_graph)) as Box<Iterator<Item = TTFSeg>>))
                    },
                };

                SegmentIter { shortcut, shortcut_graph, range, current_index, segment_iter_state, current_source_iter, source_contained_segments: false, cached_iter: None }
            }
        }
        // println!("new shortcut iter range {:?}, index: {}, iter created? {}", range, current_index, current_source_iter.is_some());
    }

    fn calc_next(&mut self) -> Option<<Self as Iterator>::Item> {
        // println!("shortcut next");
        match self.current_source_iter {
            Some(_) => {
                // TODO move borrow into Some(...) match once NLL are more stable
                match self.current_source_iter.as_mut().unwrap().next() {
                    Some(segment) => {
                        // debug_assert!(abs_diff(ipp.1, self.shortcut.evaluate(ipp.0, self.shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", ipp.0, ipp.1, self.shortcut.evaluate(ipp.0, self.shortcut_graph), self.shortcut.debug_to_s(self.shortcut_graph, 0));
                        debug_assert!(self.range.contains(segment.valid.start));
                        debug_assert!(self.range.contains(segment.valid.end) || segment.valid.end == self.range.end());
                        self.source_contained_segments = true;
                        Some(segment)
                    },
                    None => {
                        if !self.source_contained_segments {
                            if let ShortcutPaths::Multi(_) = self.shortcut.data {
                                let segment_range = self.shortcut.segment_range(self.current_index);
                                let value = self.shortcut.evaluate(segment_range.start, self.shortcut_graph);
                                self.source_contained_segments = true;
                                return Some(TTFSeg::new((segment_range.start, value), (segment_range.end, value)))
                            }
                        }

                        self.current_source_iter = None;

                        match self.segment_iter_state {
                            InitialPathSegmentIterState::InitialCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.num_segments();
                                if self.current_index != initial_index {
                                    self.next()
                                } else {
                                    None
                                }
                            },
                            InitialPathSegmentIterState::InitialPartialyCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.num_segments();
                                if self.current_index == initial_index {
                                    self.segment_iter_state = InitialPathSegmentIterState::Finishing;
                                    self.next()
                                } else {
                                    self.next()
                                }
                            },
                            InitialPathSegmentIterState::Finishing => None,
                        }
                    },
                }
            },
            None => {
                match self.shortcut.data {
                    ShortcutPaths::None => None,
                    ShortcutPaths::One(_data) => None,
                    ShortcutPaths::Multi(ref data) => {
                        let ipp = data[self.current_index].0;

                        if self.range.contains(ipp) {
                            let mut segment_range = self.shortcut.segment_range(self.current_index);
                            if self.segment_iter_state == InitialPathSegmentIterState::Finishing {
                                segment_range.end = self.range.end()
                            }
                            let segment_range = WrappingRange::new(segment_range);
                            self.current_source_iter = Some(Box::new(data[self.current_index].1.seg_iter(segment_range, self.shortcut_graph)));
                            self.source_contained_segments = false;
                            self.next()
                        } else {
                            None
                        }
                    }
                }
            },
        }
    }
}

impl<'a, 'b> Iterator for SegmentIter<'a, 'b> {
    type Item = TTFSeg;

    fn next(&mut self) -> Option<Self::Item> {
        // if let Some(ref mut iter) = self.cached_iter {
        //     iter.next().map(|it| *it)
        // } else {
            self.calc_next()
        // }
    }
}

#[derive(Debug, PartialEq)]
enum InitialPathSegmentIterState {
    InitialCompleted(usize),
    InitialPartialyCompleted(usize),
    Finishing,
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
                vec![0,  1,     3,     5],
                vec![0,  2, 6,  2, 6],
                vec![1,  1, 5,  6, 2],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(Some(2))];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let mut shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = shortcut_graph.get_upward(2).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_ipps, vec![(2,6), (6,2)]);
            let all_ipps: Vec<_> = Linked::new(1, 0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_ipps, vec![(2,2), (6,6)]);

            shortcut_graph.merge_upward(2, Linked::new(1, 0));

            assert_eq!(shortcut_graph.get_upward(2).data, ShortcutPaths::Multi(vec![(0, ShortcutData::new(ShortcutSource::Shortcut(1, 0))), (4, ShortcutData::new(ShortcutSource::OriginalEdge(2)))]));

            let all_merged_ipps: Vec<_> = shortcut_graph.get_upward(2).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_merged_ipps, vec![(0,4), (2,2), (4,4), (6,2)]);
        });
    }

    #[test]
    fn test_eval() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 3, 6],
                vec![1, 3, 9,  0, 5, 8],
                vec![2, 5, 3,  1, 2, 1],
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

    #[test]
    fn test_iter() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 3, 6],
                vec![1, 3, 9,  0, 5, 8],
                vec![2, 5, 3,  1, 2, 1],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_ipps, vec![(1,2), (3,5), (9,3)]);
        });
    }

    #[test]
    fn test_static_weight_iter() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1],
                vec![0],
                vec![0, 1],
                vec![0],
                vec![2],
            );

            let cch_first_out = vec![0, 1];
            let cch_head =      vec![0];

            let outgoing = vec![Shortcut::new(Some(0))];
            let incoming = vec![Shortcut::new(Some(0))];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).collect();
            assert_eq!(all_ipps, vec![]);
        });
    }

    #[test]
    fn test_two_static_segments_iter() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1],
                vec![0],
                vec![0, 1],
                vec![0],
                vec![2],
            );

            let cch_first_out = vec![0, 1];
            let cch_head =      vec![0];
            let outgoing = vec![Shortcut { data: ShortcutPaths::Multi(vec![(2, ShortcutData::new(ShortcutSource::OriginalEdge(0))), (6, ShortcutData::new(ShortcutSource::OriginalEdge(0)))]), cache: None }];
            let incoming = vec![Shortcut::new(Some(0))];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_ipps, vec![(2,2), (6,2)]);
        });
    }

    #[test]
    fn test_wrapping_segments_iter() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2],
                vec![1, 0],
                vec![0, 1, 4],
                vec![0, 0, 6, 7],
                vec![1, 1, 3, 2],
            );

            let cch_first_out = vec![0, 1, 2];
            let cch_head =      vec![1, 0];
            let outgoing = vec![Shortcut { data: ShortcutPaths::Multi(vec![(2, ShortcutData::new(ShortcutSource::OriginalEdge(0))), (5, ShortcutData::new(ShortcutSource::OriginalEdge(1)))]), cache: None }, Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }), &shortcut_graph).map(TTIpp::as_tuple).collect();
            assert_eq!(all_ipps, vec![(0,1), (2,1), (5,2), (6,3), (7,2)]);
        });
    }
}
