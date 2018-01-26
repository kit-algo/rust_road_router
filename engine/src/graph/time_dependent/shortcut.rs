use super::*;

#[derive(Debug)]
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

    pub fn shorten(&mut self, down: EdgeId, up: EdgeId, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) {
        if self.source_data.is_empty() {
            self.source_data.push(ShortcutData::new(ShortcutSource::Shortcut(down, up)));
            self.time_data.push(0);
        } else {
            let other = Linked::new(down, up);

            // TODO bounds for range
            let (current_lower_bound, current_upper_bound) = self.bounds(original_graph, shortcut_graph);
            let (other_lower_bound, other_upper_bound) = other.bounds(original_graph, shortcut_graph);
            if current_lower_bound >= other_upper_bound {
                self.source_data.clear();
                self.time_data.clear();
                self.source_data.push(ShortcutData::new(ShortcutSource::Shortcut(down, up)));
                self.time_data.push(0);
                return
            } else if other_lower_bound >= current_upper_bound {
                return
            }

            let current_initial_value = self.evaluate(0, original_graph, shortcut_graph);
            let other_initial_value = other.evaluate(0, original_graph, shortcut_graph);

            let is_current_initially_lower = current_initial_value <= other_initial_value;
            let mut is_current_lower_for_prev_ipp = is_current_initially_lower;
            let mut self_next_ipp = self.next_ipp_greater_eq(0, original_graph, shortcut_graph);
            let mut other_next_ipp = other.next_ipp_greater_eq(0, original_graph, shortcut_graph);
            let mut better_way = vec![];

            while self_next_ipp.is_some() || other_next_ipp.is_some() {
                let ipp = match (self_next_ipp, other_next_ipp) {
                    (Some(self_next_ipp_at), Some(other_next_ipp_at)) => {
                        if self_next_ipp_at <= other_next_ipp_at {
                            self_next_ipp = self.next_ipp_greater_eq(self_next_ipp_at + 1, original_graph, shortcut_graph);
                            self_next_ipp_at
                        } else {
                            other_next_ipp = other.next_ipp_greater_eq(other_next_ipp_at + 1, original_graph, shortcut_graph);
                            other_next_ipp_at
                        }
                    },
                    (None, Some(other_next_ipp_at)) => {
                        other_next_ipp = other.next_ipp_greater_eq(other_next_ipp_at + 1, original_graph, shortcut_graph);
                        other_next_ipp_at
                    },
                    (Some(self_next_ipp_at), None) => {
                        self_next_ipp = self.next_ipp_greater_eq(self_next_ipp_at + 1, original_graph, shortcut_graph);
                        self_next_ipp_at
                    },
                    (None, None) => panic!("while loop should have terminated")
                };

                let self_next_ipp_value = self.evaluate(ipp, original_graph, shortcut_graph);
                let other_next_ipp_value = other.evaluate(ipp, original_graph, shortcut_graph);

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
                            new_shortcut.source_data.push(ShortcutData::new(ShortcutSource::Shortcut(down, up)));
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

            self.source_data = new_shortcut.source_data;
            self.time_data = new_shortcut.time_data;
        }
    }

    pub fn evaluate(&self, departure: Timestamp, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) -> Weight {
        if self.source_data.is_empty() { return INFINITY }
        match self.time_data.binary_search(&departure) {
            Ok(index) => self.source_data[index].evaluate(departure, original_graph, shortcut_graph),
            Err(index) => {
                let index = (index + self.source_data.len() - 1) % self.source_data.len();
                self.source_data[index].evaluate(departure, original_graph, shortcut_graph)
            },
        }
    }

    pub fn next_ipp_greater_eq(&self, time: Timestamp, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) -> Option<Timestamp> {
        if self.source_data.is_empty() { return None }
        match self.time_data.binary_search(&time) {
            Ok(_) => Some(time),
            Err(index) => {
                let index = (index + self.source_data.len() - 1) % self.source_data.len();
                self.source_data[index].next_ipp_greater_eq(time, original_graph, shortcut_graph)
            },
        }
    }

    pub fn bounds(&self, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        let (mins, maxs): (Vec<Weight>, Vec<Weight>) = self.source_data.iter().map(|source| source.bounds(original_graph, shortcut_graph)).unzip();
        (mins.into_iter().min().unwrap_or(INFINITY), maxs.into_iter().max().unwrap_or(INFINITY))
    }
}

