use super::*;
use benchmark::measure;

#[derive(Debug, Clone)]
pub struct Shortcut {
    source_data: Vec<ShortcutData>,
    time_data: Vec<Timestamp>
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
        Shortcut { source_data, time_data }
    }

    pub fn merge(&self, other: Linked, shortcut_graph: &ShortcutGraph) -> Shortcut {
        if !other.is_valid_path(shortcut_graph) {
            return self.clone()
        }
        if self.source_data.is_empty() {
            return Shortcut { source_data: vec![other.as_shortcut_data()], time_data: vec![0] }
        }

        // TODO bounds for range
        let (current_lower_bound, current_upper_bound) = self.bounds(shortcut_graph);
        let (other_lower_bound, other_upper_bound) = other.bounds(shortcut_graph);
        if current_lower_bound >= other_upper_bound {
            return Shortcut { source_data: vec![other.as_shortcut_data()], time_data: vec![0] }
        } else if other_lower_bound >= current_upper_bound {
            return self.clone()
        }

        println!("actual work: {}", self.num_segments());

        let current_initial_value = self.evaluate(0, shortcut_graph);
        let other_initial_value = other.evaluate(0, shortcut_graph);

        let is_current_initially_lower = current_initial_value <= other_initial_value;
        let mut is_current_lower_for_prev_ipp = is_current_initially_lower;

        let range = WrappingRange::new(Range { start: 0, end: 0 }, shortcut_graph.original_graph().period());
        let mut self_iter = self.ipp_iter(range.clone(), shortcut_graph);
        let mut other_iter = other.ipp_iter(range, shortcut_graph);

        let mut self_next_ipp = self_iter.next();
        let mut other_next_ipp = other_iter.next();
        let mut better_way = Vec::new();
        let mut ipp_counter = 0;

        let mut switched = false;

        measure("combined ipp iteration", || {
            while self_next_ipp.is_some() || other_next_ipp.is_some() {
                let (ipp, self_next_ipp_value, other_next_ipp_value) = match (self_next_ipp, other_next_ipp) {
                    (Some((self_next_ipp_at, self_next_ipp_value)), Some((other_next_ipp_at, other_next_ipp_value))) => {
                        if self_next_ipp_at <= other_next_ipp_at {
                            self_next_ipp = self_iter.next();
                            (self_next_ipp_at, self_next_ipp_value, other.evaluate(self_next_ipp_at, shortcut_graph))
                        } else {
                            other_next_ipp = other_iter.next();
                            (other_next_ipp_at, self.evaluate(other_next_ipp_at, shortcut_graph), other_next_ipp_value)
                        }
                    },
                    (None, Some((other_next_ipp_at, other_next_ipp_value))) => {
                        other_next_ipp = other_iter.next();
                        (other_next_ipp_at, self.evaluate(other_next_ipp_at, shortcut_graph), other_next_ipp_value)
                    },
                    (Some((self_next_ipp_at, self_next_ipp_value)), None) => {
                        self_next_ipp = self_iter.next();
                        (self_next_ipp_at, self_next_ipp_value, other.evaluate(self_next_ipp_at, shortcut_graph))
                    },
                    (None, None) => panic!("while loop should have terminated")
                };

                let is_current_lower_for_ipp = self_next_ipp_value <= other_next_ipp_value;
                if is_current_lower_for_ipp != is_current_lower_for_prev_ipp {
                    switched = true;
                    better_way.push((is_current_lower_for_ipp, ipp));
                }

                is_current_lower_for_prev_ipp = is_current_lower_for_ipp;
                ipp_counter += 1;
            }
        });

        println!("iterated ipps: {:?}", ipp_counter);
        let mut new_shortcut = Shortcut { source_data: vec![], time_data: vec![] };

        // TODO schnittpunkte switch???
        let initial_better_way = better_way.last().map(|way| way.0).unwrap_or(is_current_initially_lower);
        let mut current_better_way = initial_better_way;
        let mut better_way_iter = better_way.iter();
        let mut next_better_way = better_way_iter.next();
        {
            let mut prev_source = self.source_data.last().unwrap();

            for (&time, source) in self.time_data.iter().zip(self.source_data.iter()) {
                if next_better_way.is_some() && time >= next_better_way.unwrap().1 {
                    debug_assert_ne!(current_better_way, next_better_way.unwrap().0);
                    current_better_way = next_better_way.unwrap().0;
                    next_better_way = better_way_iter.next();

                    if current_better_way {
                        new_shortcut.source_data.push(*prev_source);
                    } else {
                        new_shortcut.source_data.push(other.as_shortcut_data());
                    }
                    new_shortcut.time_data.push(next_better_way.unwrap().1);
                }

                if current_better_way {
                    new_shortcut.source_data.push(*source);
                    new_shortcut.time_data.push(time);
                }
                prev_source = source;
            }
        }
        if switched {
            assert!(new_shortcut.num_segments() > 1);
        }

        new_shortcut
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
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

    fn segment_range(&self, segment: usize) -> Range<Timestamp> {
        let next_index = (segment + 1) % self.source_data.len();
        Range { start: self.time_data[segment], end: self.time_data[next_index] }
    }
}

pub struct Iter<'a, 'b: 'a> {
    shortcut: &'b Shortcut,
    shortcut_graph: &'a ShortcutGraph<'a>,
    range: WrappingRange<Timestamp>,
    current_index: usize,
    initial_index: usize,
    current_source_iter: Option<Box<Iterator<Item = (Timestamp, Weight)> + 'a>>
}

impl<'a, 'b> Iter<'a, 'b> {
    fn new(shortcut: &'b Shortcut, range: WrappingRange<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        let (current_index, current_source_iter) = match shortcut.time_data.binary_search(range.start()) {
            Ok(index) => (index, None),
            Err(index) => {
                let current_index = (index + shortcut.source_data.len() - 1) % shortcut.source_data.len();
                let mut segment_range = WrappingRange::new(shortcut.segment_range(current_index), *range.wrap_around());
                segment_range.shift_start(*range.start());
                // println!("shortcut segment iter range {:?}", segment_range);
                (current_index, Some(Box::new(shortcut.source_data[current_index].ipp_iter(segment_range, shortcut_graph)) as Box<Iterator<Item = (Timestamp, Weight)>>))
            },
        };

        // println!("new shortcut iter range {:?}, index: {}, iter created? {}", range, current_index, current_source_iter.is_some());
        Iter { shortcut, shortcut_graph, range, current_index, initial_index: current_index, current_source_iter }
    }
}

impl<'a, 'b> Iterator for Iter<'a, 'b> {
    type Item = (Timestamp, Weight);

    fn next(&mut self) -> Option<Self::Item> {
        // println!("shortcut next");
        match self.current_source_iter {
            Some(_) => {
                // TODO move borrow into Some(...) match once NLL are more stable
                match self.current_source_iter.as_mut().unwrap().next() {
                    Some(ipp) => {
                        debug_assert_eq!(ipp.1, self.shortcut.evaluate(ipp.0, self.shortcut_graph));
                        if self.range.contains(ipp.0) {
                            // println!("shortcut result {}", ipp);
                            Some(ipp)
                        } else {
                            None
                        }
                    },
                    None => {
                        self.current_source_iter = None;
                        self.current_index = (self.current_index + 1) % self.shortcut.source_data.len();
                        if self.current_index != self.initial_index {
                            self.next()
                        } else {
                            None
                        }
                    },
                }
            },
            None => {
                let ipp = self.shortcut.time_data[self.current_index];

                if self.range.contains(ipp) {
                    let mut segment_range = self.shortcut.segment_range(self.current_index);
                    if self.shortcut.source_data.len() > 1 {
                        segment_range.start = (segment_range.start + 1) % *self.range.wrap_around();
                    }
                    let segment_range = WrappingRange::new(segment_range, *self.range.wrap_around());
                    self.current_source_iter = Some(Box::new(self.shortcut.source_data[self.current_index].ipp_iter(segment_range, self.shortcut_graph)));
                    if self.shortcut.source_data.len() == 1 {
                        self.next()
                    } else {
                        Some((ipp, self.shortcut.evaluate(ipp, self.shortcut_graph))) // TODO optimize?
                    }
                } else {
                    None
                }
            },
        }
    }
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

        assert_eq!(new_shortcut.time_data, vec![0, 4]);
        assert_eq!(new_shortcut.source_data, vec![ShortcutData::new(ShortcutSource::Shortcut(1, 0)), ShortcutData::new(ShortcutSource::OriginalEdge(2))]);
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
        assert_eq!(all_ipps, vec![(0,2)]);
    }
}
