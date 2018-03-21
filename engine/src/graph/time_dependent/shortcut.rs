use super::*;

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

        let current_initial_value = self.evaluate(0, shortcut_graph);
        let other_initial_value = other.evaluate(0, shortcut_graph);

        let is_current_initially_lower = current_initial_value <= other_initial_value;
        let mut is_current_lower_for_prev_ipp = is_current_initially_lower;

        let range = Range { start: 0, end: shortcut_graph.original_graph().period() };
        let mut self_iter = self.ipp_iter(range.clone(), shortcut_graph);
        let mut other_iter = other.ipp_iter(range, shortcut_graph);

        let mut self_next_ipp = self_iter.next();
        let mut other_next_ipp = other_iter.next();
        let mut better_way = Vec::new();

        while self_next_ipp.is_some() || other_next_ipp.is_some() {
            let ipp = match (self_next_ipp, other_next_ipp) {
                (Some(self_next_ipp_at), Some(other_next_ipp_at)) => {
                    if self_next_ipp_at <= other_next_ipp_at {
                        self_next_ipp = self_iter.next();
                        self_next_ipp_at
                    } else {
                        other_next_ipp = other_iter.next();
                        other_next_ipp_at
                    }
                },
                (None, Some(other_next_ipp_at)) => {
                    other_next_ipp = other_iter.next();
                    other_next_ipp_at
                },
                (Some(self_next_ipp_at), None) => {
                    self_next_ipp = self_iter.next();
                    self_next_ipp_at
                },
                (None, None) => panic!("while loop should have terminated")
            };

            let self_next_ipp_value = self.evaluate(ipp, shortcut_graph);
            let other_next_ipp_value = other.evaluate(ipp, shortcut_graph);

            let is_current_lower_for_ipp = self_next_ipp_value <= other_next_ipp_value;
            if is_current_lower_for_ipp != is_current_lower_for_prev_ipp {
                better_way.push((is_current_lower_for_ipp, ipp));
            }

            is_current_lower_for_prev_ipp = is_current_lower_for_ipp;
        }

        let mut new_shortcut = Shortcut { source_data: vec![], time_data: vec![] };

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

    pub fn ipp_iter<'a, 'b: 'a>(&'b self, range: Range<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        Iter::new(&self, WrappingRange::new(range, shortcut_graph.original_graph().period()), shortcut_graph)
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
}

pub struct Iter<'a, 'b: 'a> {
    shortcut: &'b Shortcut,
    shortcut_graph: &'a ShortcutGraph<'a>,
    range: WrappingRange<Timestamp>,
    current_index: usize,
    initial_index: usize,
    current_source_iter: Option<Box<Iterator<Item = Timestamp> + 'a>>
}

impl<'a, 'b> Iter<'a, 'b> {
    fn new(shortcut: &'b Shortcut, range: WrappingRange<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a, 'b> {
        let (current_index, current_source_iter) = match shortcut.time_data.binary_search(range.start()) {
            Ok(index) => (index, None),
            Err(index) => {
                let current_index = (index + shortcut.source_data.len() - 1) % shortcut.source_data.len();
                let next_index = (current_index + 1) % shortcut.source_data.len();
                (current_index, Some(Box::new(shortcut.source_data[current_index].ipp_iter(Range { start: *range.start(), end: shortcut.time_data[next_index] }, shortcut_graph)) as Box<Iterator<Item = Timestamp>>))
            },
        };

        Iter { shortcut, shortcut_graph, range, current_index, initial_index: current_index, current_source_iter }
    }
}

impl<'a, 'b> Iterator for Iter<'a, 'b> {
    type Item = Timestamp;

    fn next(&mut self) -> Option<Timestamp> {
        match self.current_source_iter {
            Some(_) => {
                // TODO move borrow into Some(...) match once NLL are more stable
                match self.current_source_iter.as_mut().unwrap().next() {
                    Some(ipp) => {
                        if self.range.contains(ipp) {
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
                    let next_index = (self.current_index + 1) % self.shortcut.source_data.len();
                    self.current_source_iter = Some(Box::new(self.shortcut.source_data[self.current_index].ipp_iter(Range { start: (ipp + 1) % self.shortcut_graph.original_graph().period(), end: self.shortcut.time_data[next_index] }, self.shortcut_graph)));
                    Some(ipp)
                } else {
                    None
                }
            },
        }
    }
}
