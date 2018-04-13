use super::*;
use benchmark::measure;

#[derive(Debug, Clone)]
pub struct Shortcut {
    source_data: Vec<ShortcutData>,
    time_data: Vec<Timestamp>,
    cache: Option<Vec<(Timestamp, Weight)>>
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>) -> Shortcut {
        // TODO with capacity 1 if original edge???
        let mut source_data = Vec::new();
        let mut time_data = Vec::new();
        if let Some(edge) = source {
            source_data.push(ShortcutData::new(ShortcutSource::OriginalEdge(edge)));
            time_data.push(0);
        }
        Shortcut { source_data, time_data, cache: None }
    }

    pub fn merge(&self, other: Linked, shortcut_graph: &ShortcutGraph) -> Shortcut {
        if !other.is_valid_path(shortcut_graph) {
            return self.clone()
        }
        if self.source_data.is_empty() {
            return Shortcut { source_data: vec![other.as_shortcut_data()], time_data: vec![0], cache: None }
        }

        // TODO bounds for range
        let (current_lower_bound, current_upper_bound) = self.bounds(shortcut_graph);
        let (other_lower_bound, other_upper_bound) = other.bounds(shortcut_graph);
        if current_lower_bound >= other_upper_bound {
            return Shortcut { source_data: vec![other.as_shortcut_data()], time_data: vec![0], cache: None }
        } else if other_lower_bound >= current_upper_bound {
            return self.clone()
        }

        let mut ipp_counter = 0;

        let mut intersections =
            // measure("combined ipp iteration and intersecting", ||
        {
            let range = WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.original_graph().period());
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

            intersections(ipp_iter, || self.evaluate(0, shortcut_graph), || other.evaluate(0, shortcut_graph), shortcut_graph.original_graph().period())
        }
        // )
        ;

        // println!("iterated ipps: {:?}", ipp_counter);

        if intersections.is_empty() {
            if self.evaluate(0, shortcut_graph) <= other.evaluate(0, shortcut_graph) {
                return self.clone()
            } else {
                return Shortcut { source_data: vec![other.as_shortcut_data()], time_data: vec![0], cache: None }
            }
        }

        let c = intersections.len();
        debug_assert_eq!(c % 2, 0);

        let period = shortcut_graph.original_graph().period();

        for &(intersection, is_self_better) in intersections.iter() {
            let before_intersection = if intersection < 1 { intersection + period - 1 } else { intersection - 1 };
            let debug_output = || {
                // let collected: Vec<(Timestamp, IppSource)> = MergingIter {
                //     shortcut_iter: self.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.original_graph().period()), shortcut_graph).peekable(),
                //     linked_iter: other.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.original_graph().period()), shortcut_graph).peekable()
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
        intersections.last_mut().unwrap().0 += period;

        let mut new_shortcut = Shortcut { source_data: vec![], time_data: vec![], cache: None };

        let mut intersections = intersections.into_iter().peekable();
        let (_, mut is_self_currently_better) = intersections.next().unwrap();
        let mut prev_source = self.source_data.last().unwrap();

        for (&time, source) in self.time_data.iter().zip(self.source_data.iter()) {
            // println!("time: {}, source: {:?}", time, source);
            while time >= intersections.peek().unwrap().0 {
                let (peeked_ipp, self_better_at_peeked) = intersections.next().unwrap();
                // println!("peeked {} {}", peeked_ipp, self_better_at_peeked);

                if self_better_at_peeked {
                    // println!("self better");
                    if time > peeked_ipp {
                        // println!("pushing prev source {:?}", prev_source);
                        new_shortcut.time_data.push(peeked_ipp);
                        new_shortcut.source_data.push(*prev_source);
                    }
                } else {
                    // println!("other better - pushing other {:?}", other.as_shortcut_data());
                    new_shortcut.time_data.push(peeked_ipp);
                    new_shortcut.source_data.push(other.as_shortcut_data());
                }

                is_self_currently_better = self_better_at_peeked;
            }

            if is_self_currently_better {
                // println!("applying current source: {:?}", *source);
                new_shortcut.source_data.push(*source);
                new_shortcut.time_data.push(time);
            }

            prev_source = source;
        }

        for (intersection_ipp, segment_self_better) in intersections {
            if segment_self_better {
                // println!("adding rest self {} {:?}", intersection_ipp, *prev_source);
                new_shortcut.time_data.push(intersection_ipp);
                new_shortcut.source_data.push(*prev_source);
            } else {
                // println!("adding rest other {} {:?}", intersection_ipp, other.as_shortcut_data());
                new_shortcut.time_data.push(intersection_ipp);
                new_shortcut.source_data.push(other.as_shortcut_data());
            }
        }
        new_shortcut.time_data.pop();
        new_shortcut.source_data.pop();

        for values in new_shortcut.time_data.windows(2) {
            debug_assert!(values[0] < values[1]);
        }

        new_shortcut
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < shortcut_graph.original_graph().period());
        if self.source_data.is_empty() { return INFINITY }
        match self.time_data.binary_search(&departure) {
            Ok(index) => self.source_data[index].evaluate(departure, shortcut_graph),
            Err(index) => {
                let index = (index + self.source_data.len() - 1) % self.source_data.len();
                self.source_data[index].evaluate(departure, shortcut_graph)
            },
        }
    }

    pub fn ipp_iter<'a, 'b: 'a>(&'b self, range: WrappingRange<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        Iter::new(&self, range, shortcut_graph)
    }

    pub fn bounds(&self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        let (mins, maxs): (Vec<Weight>, Vec<Weight>) = self.source_data.iter().map(|source| source.bounds(shortcut_graph)).unzip();
        (mins.into_iter().min().unwrap_or(INFINITY), maxs.into_iter().max().unwrap_or(INFINITY))
    }

    pub fn num_segments(&self) -> usize {
        self.source_data.len()
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_segments() > 0
    }

    pub fn cache_ipps(&mut self, shortcut_graph: &ShortcutGraph) {
        let range = WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.original_graph().period());
        let ipps = self.ipp_iter(range, shortcut_graph).collect();
        self.cache = Some(ipps);
    }

    pub fn clear_cache(&mut self) {
        self.cache = None
    }

    fn segment_range(&self, segment: usize) -> Range<Timestamp> {
        let next_index = (segment + 1) % self.source_data.len();
        Range { start: self.time_data[segment], end: self.time_data[next_index] }
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        let mut s = String::from("Shortcut: ");
        for (time, source) in self.time_data.iter().zip(self.source_data.iter()) {
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

struct MergingIter<'a, 'b: 'a> {
    shortcut_iter: Peekable<Iter<'a, 'b>>,
    linked_iter: Peekable<linked::Iter<'a>>
}

impl<'a, 'b> Iterator for MergingIter<'a, 'b> {
    type Item = (Timestamp, IppSource);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.shortcut_iter.peek(), self.linked_iter.peek()) {
            (Some(&(self_next_ipp_at, self_next_ipp_value)), Some(&(other_next_ipp_at, other_next_ipp_value))) => {
                if self_next_ipp_at == other_next_ipp_at {
                    self.shortcut_iter.next();
                    self.linked_iter.next();
                    Some((self_next_ipp_at, IppSource::Both(self_next_ipp_value, other_next_ipp_value)))
                } else if self_next_ipp_at <= other_next_ipp_at {
                    self.shortcut_iter.next();
                    Some((self_next_ipp_at, IppSource::First(self_next_ipp_value)))
                } else {
                    self.linked_iter.next();
                    Some((other_next_ipp_at, IppSource::Second(other_next_ipp_value)))
                }
            },
            (None, Some(&(other_next_ipp_at, other_next_ipp_value))) => {
                self.linked_iter.next();
                Some((other_next_ipp_at, IppSource::Second(other_next_ipp_value)))
            },
            (Some(&(self_next_ipp_at, self_next_ipp_value)), None) => {
                self.shortcut_iter.next();
                Some((self_next_ipp_at, IppSource::First(self_next_ipp_value)))
            },
            (None, None) => None
        }
    }
}

pub struct Iter<'a, 'b: 'a> {
    shortcut: &'b Shortcut,
    shortcut_graph: &'a ShortcutGraph<'a>,
    range: WrappingRange<Timestamp>,
    current_index: usize,
    segment_iter_state: InitialSegmentIterState,
    current_source_iter: Option<Box<Iterator<Item = (Timestamp, Weight)> + 'a>>,
    cached_iter: Option<WrappingSliceIter<'b>>,
}

impl<'a, 'b> Iter<'a, 'b> {
    fn new(shortcut: &'b Shortcut, range: WrappingRange<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        if !shortcut.is_valid_path() {
            return Iter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialSegmentIterState::Finishing, current_source_iter: None, cached_iter: None }
        }

        if let Some(ref cache) = shortcut.cache {
            let cached_iter = WrappingSliceIter::new(cache, range.clone());
            return Iter { shortcut, shortcut_graph, range, current_index: 0, segment_iter_state: InitialSegmentIterState::Finishing, current_source_iter: None, cached_iter: Some(cached_iter) }
        }

        let (current_index, segment_iter_state, current_source_iter) = match shortcut.time_data.binary_search(range.start()) {
            Ok(index) => (index, InitialSegmentIterState::InitialCompleted(index), None),
            Err(index) => {
                let current_index = (index + shortcut.source_data.len() - 1) % shortcut.source_data.len();
                let mut segment_range = WrappingRange::new(shortcut.segment_range(current_index), *range.wrap_around());
                let segment_iter_state = if segment_range.full_range() {
                    InitialSegmentIterState::InitialCompleted(current_index)
                } else {
                    InitialSegmentIterState::InitialPartialyCompleted(current_index)
                };
                segment_range.shift_start(*range.start());
                // println!("shortcut segment iter range {:?}", segment_range);
                (current_index, segment_iter_state, Some(Box::new(shortcut.source_data[current_index].ipp_iter(segment_range, shortcut_graph)) as Box<Iterator<Item = (Timestamp, Weight)>>))
            },
        };

        // println!("new shortcut iter range {:?}, index: {}, iter created? {}", range, current_index, current_source_iter.is_some());
        Iter { shortcut, shortcut_graph, range, current_index, segment_iter_state, current_source_iter, cached_iter: None }
    }

    fn calc_next(&mut self) -> Option<<Self as Iterator>::Item> {
        // println!("shortcut next");
        match self.current_source_iter {
            Some(_) => {
                // TODO move borrow into Some(...) match once NLL are more stable
                match self.current_source_iter.as_mut().unwrap().next() {
                    Some(ipp) => {
                        debug_assert!(abs_diff(ipp.1, self.shortcut.evaluate(ipp.0, self.shortcut_graph)) < TOLERANCE, "at: {} was: {} but should have been: {}. {}", ipp.0, ipp.1, self.shortcut.evaluate(ipp.0, self.shortcut_graph), self.shortcut.debug_to_s(self.shortcut_graph, 0));
                        if self.range.contains(ipp.0) {
                            // println!("shortcut result {}", ipp);
                            Some(ipp)
                        } else {
                            None
                        }
                    },
                    None => {
                        self.current_source_iter = None;
                        match self.segment_iter_state {
                            InitialSegmentIterState::InitialCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.source_data.len();
                                if self.current_index != initial_index {
                                    self.next()
                                } else {
                                    None
                                }
                            },
                            InitialSegmentIterState::InitialPartialyCompleted(initial_index) => {
                                self.current_index = (self.current_index + 1) % self.shortcut.source_data.len();
                                if self.current_index == initial_index {
                                    self.segment_iter_state = InitialSegmentIterState::Finishing;
                                    self.next()
                                } else {
                                    self.next()
                                }
                            },
                            InitialSegmentIterState::Finishing => None,
                        }
                    },
                }
            },
            None => {
                if !self.shortcut.is_valid_path() { return None }

                let ipp = self.shortcut.time_data[self.current_index];

                if self.range.contains(ipp) {
                    let mut segment_range = self.shortcut.segment_range(self.current_index);
                    if self.shortcut.source_data.len() > 1 {
                        segment_range.start = (segment_range.start + 1) % *self.range.wrap_around();
                    }
                    if self.segment_iter_state == InitialSegmentIterState::Finishing {
                        segment_range.end = *self.range.end()
                    }
                    let segment_range = WrappingRange::new(segment_range, *self.range.wrap_around());
                    self.current_source_iter = Some(Box::new(self.shortcut.source_data[self.current_index].ipp_iter(segment_range, self.shortcut_graph)));
                    if self.shortcut.source_data.len() == 1 {
                        self.next()
                    } else {
                        Some((ipp, self.shortcut.source_data[self.current_index].evaluate(ipp, self.shortcut_graph))) // TODO optimize?
                    }
                } else {
                    None
                }
            },
        }
    }
}

impl<'a, 'b> Iterator for Iter<'a, 'b> {
    type Item = (Timestamp, Weight);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.cached_iter {
            iter.next().map(|it| *it)
        } else {
            self.calc_next()
        }
    }
}

#[derive(Debug, PartialEq)]
enum InitialSegmentIterState {
    InitialCompleted(usize),
    InitialPartialyCompleted(usize),
    Finishing,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merging() {
        let graph = TDGraph::new(
            vec![0, 1,    3, 3],
            vec![2, 0, 2],
            vec![0,  1,     3,     5],
            vec![0,  2, 6,  2, 6],
            vec![1,  1, 5,  6, 2],
            8
        );

        let cch_first_out = vec![0, 1, 3, 3];
        let cch_head =      vec![2, 0, 2];

        let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(Some(2))];
        let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);
        let new_shortcut = shortcut_graph.get_upward(2).merge(Linked::new(1, 0), &shortcut_graph);

        let all_ipps: Vec<(Timestamp, Weight)> = shortcut_graph.get_upward(2).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 8), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![(2,6), (6,2)]);
        let all_ipps: Vec<(Timestamp, Weight)> = Linked::new(1, 0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 8), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![(2,2), (6,6)]);

        assert_eq!(new_shortcut.time_data, vec![0, 4]);
        assert_eq!(new_shortcut.source_data, vec![ShortcutData::new(ShortcutSource::Shortcut(1, 0)), ShortcutData::new(ShortcutSource::OriginalEdge(2))]);

        let all_merged_ipps: Vec<(Timestamp, Weight)> = new_shortcut.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 8), &shortcut_graph).collect();
        assert_eq!(all_merged_ipps, vec![(0,4), (2,2), (4,4), (6,2)]);
    }

    #[test]
    fn test_eval() {
        let graph = TDGraph::new(
            vec![0, 1, 2, 2],
            vec![2, 0],
            vec![0, 3, 6],
            vec![1, 3, 9,  0, 5, 8],
            vec![2, 5, 3,  1, 2, 1],
            10
        );

        let cch_first_out = vec![0, 1, 3, 3];
        let cch_head =      vec![2, 0, 2];

        let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
        let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        assert_eq!(shortcut_graph.get_downward(1).evaluate(0, &shortcut_graph), 1);
        assert_eq!(shortcut_graph.get_upward(0).evaluate(1, &shortcut_graph), 2);
    }

    #[test]
    fn test_iter() {
        let graph = TDGraph::new(
            vec![0, 1, 2, 2],
            vec![2, 0],
            vec![0, 3, 6],
            vec![1, 3, 9,  0, 5, 8],
            vec![2, 5, 3,  1, 2, 1],
            10
        );

        let cch_first_out = vec![0, 1, 3, 3];
        let cch_head =      vec![2, 0, 2];

        let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
        let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        let all_ipps: Vec<(Timestamp, Weight)> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 10), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![(1,2), (3,5), (9,3)]);
    }

    #[test]
    fn test_static_weight_iter() {
        let graph = TDGraph::new(
            vec![0, 1],
            vec![0],
            vec![0, 1],
            vec![0],
            vec![2],
            10
        );

        let cch_first_out = vec![0, 1];
        let cch_head =      vec![0];

        let outgoing = vec![Shortcut::new(Some(0))];
        let incoming = vec![Shortcut::new(Some(0))];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        let all_ipps: Vec<(Timestamp, Weight)> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 10), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![]);
    }

    #[test]
    fn test_two_static_segments_iter() {
        let graph = TDGraph::new(
            vec![0, 1],
            vec![0],
            vec![0, 1],
            vec![0],
            vec![2],
            10
        );

        let cch_first_out = vec![0, 1];
        let cch_head =      vec![0];
        let outgoing = vec![Shortcut { time_data: vec![2, 6], source_data: vec![ShortcutData::new(ShortcutSource::OriginalEdge(0)), ShortcutData::new(ShortcutSource::OriginalEdge(0))], cache: None }];
        let incoming = vec![Shortcut::new(Some(0))];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        let all_ipps: Vec<(Timestamp, Weight)> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 10), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![(2,2), (6,2)]);
    }

    #[test]
    fn test_wrapping_segments_iter() {
        let graph = TDGraph::new(
            vec![0, 1, 2],
            vec![1, 0],
            vec![0, 1, 4],
            vec![0, 0, 6, 7],
            vec![1, 1, 3, 2],
            10
        );

        let cch_first_out = vec![0, 1, 2];
        let cch_head =      vec![1, 0];
        let outgoing = vec![Shortcut { time_data: vec![2, 5], source_data: vec![ShortcutData::new(ShortcutSource::OriginalEdge(0)), ShortcutData::new(ShortcutSource::OriginalEdge(1))], cache: None }, Shortcut::new(None)];
        let incoming = vec![Shortcut::new(None), Shortcut::new(None)];

        let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

        let all_ipps: Vec<(Timestamp, Weight)> = shortcut_graph.get_upward(0).ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 10), &shortcut_graph).collect();
        assert_eq!(all_ipps, vec![(0,1), (2,1), (5,2), (6,3), (7,2)]);
    }
}
