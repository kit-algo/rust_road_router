use super::*;

#[derive(Debug)]
pub struct Linked {
    first: EdgeId,
    second: EdgeId
}

impl Linked {
    pub fn new(first: EdgeId, second: EdgeId) -> Linked {
        Linked { first, second }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        let first_edge = shortcut_graph.get_downward(self.first);
        let second_edge = shortcut_graph.get_upward(self.second);
        second_edge.evaluate(departure + first_edge.evaluate(departure, shortcut_graph), shortcut_graph)
    }

    pub fn bounds(&self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        let (first_min, first_max) = shortcut_graph.get_downward(self.first).bounds(shortcut_graph);
        let (second_min, second_max) = shortcut_graph.get_upward(self.second).bounds(shortcut_graph);
        (first_min + second_min, first_max + second_max)
    }

    pub fn ipp_iter<'a>(&self, range: Range<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a> {
        let first_edge = shortcut_graph.get_downward(self.first);
        let second_edge = shortcut_graph.get_upward(self.second);
        Iter::new(first_edge, second_edge, range, shortcut_graph)
    }

    pub fn as_shortcut_data(&self) -> ShortcutData {
        ShortcutData::new(ShortcutSource::Shortcut(self.first, self.second))
    }
}

pub struct Iter<'a> {
    first_edge: &'a Shortcut,
    first_iter: shortcut::Iter<'a, 'a>,
    second_iter: std::iter::Peekable<shortcut::Iter<'a, 'a>>,
    range: WrappingRange<Timestamp>,

    shortcut_graph: &'a ShortcutGraph<'a>,

    first_edge_prev_ipp: (Timestamp, Weight),
    first_edge_next_ipp: Option<(Timestamp, Weight)>,
}

impl<'a> Iter<'a> {
    fn new(first_edge: &'a Shortcut, second_edge: &'a Shortcut, range: Range<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> Iter<'a> {
        let mut first_iter = first_edge.ipp_iter(range.clone(), shortcut_graph);

        let first_edge_initial_value = first_edge.evaluate(range.start, shortcut_graph);
        let second_edge_range_begin = range.start + first_edge_initial_value;

        let second_iter = second_edge.ipp_iter(Range { start: second_edge_range_begin, end: second_edge_range_begin }, shortcut_graph).peekable();

        let first_edge_next_ipp = first_iter.next().map(|ipp| (ipp, first_edge.evaluate(ipp, shortcut_graph)));

        Iter {
            first_edge, first_iter, second_iter, shortcut_graph,
            range: WrappingRange::new(range.clone(), shortcut_graph.original_graph().period()),
            first_edge_prev_ipp: (range.start, first_edge_initial_value),
            first_edge_next_ipp
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Timestamp;

    fn next(&mut self) -> Option<Timestamp> {
        match self.second_iter.peek().cloned() {
            Some(second_edge_ipp) => {
                let first_edge_next_ipp = self.first_edge_next_ipp.unwrap_or((*self.range.end(), self.first_edge.evaluate(*self.range.end(), self.shortcut_graph)));
                if WrappingRange::new(Range { start: self.first_edge_prev_ipp.0 + self.first_edge_prev_ipp.1, end: first_edge_next_ipp.0 + first_edge_next_ipp.1 }, *self.range.wrap_around()).contains(second_edge_ipp) {
                    let ipp = invert(self.first_edge_prev_ipp, first_edge_next_ipp, second_edge_ipp, self.shortcut_graph.original_graph().period());
                    self.second_iter.next();
                    Some(ipp)
                } else {
                    if let Some(first_edge_next_ipp) = self.first_edge_next_ipp {
                        self.first_edge_prev_ipp = first_edge_next_ipp;
                        self.first_edge_next_ipp = self.first_iter.next().map(|ipp| (ipp, self.first_edge.evaluate(ipp, self.shortcut_graph)));
                        Some(self.first_edge_prev_ipp.0)
                    } else {
                        None
                    }
                }
            },
            None => {
                if let Some(first_edge_next_ipp) = self.first_edge_next_ipp {
                    self.first_edge_prev_ipp = first_edge_next_ipp;
                    self.first_edge_next_ipp = self.first_iter.next().map(|ipp| (ipp, self.first_edge.evaluate(ipp, self.shortcut_graph)));
                    Some(self.first_edge_prev_ipp.0)
                } else {
                    None
                }
            },
        }
    }
}

fn invert(first_ipp: (Timestamp, Timestamp), second_ipp: (Timestamp, Timestamp), y: Timestamp, period: Timestamp) -> Timestamp {
    if first_ipp.1 == second_ipp.1 {
        return first_ipp.0
    }

    let delta_x = (period + second_ipp.0 - first_ipp.0) % period;
    let delta_y = second_ipp.1 as i64 - first_ipp.1 as i64;

    let delta_y_to_target = y as i64 - first_ipp.1 as i64;
    let delta_x_to_target = delta_y_to_target * delta_x as i64 / delta_y;
    debug_assert!(delta_x_to_target >= 0);

    first_ipp.1 + delta_x_to_target as Weight
}
