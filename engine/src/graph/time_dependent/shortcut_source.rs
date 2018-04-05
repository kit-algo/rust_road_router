use super::*;
use in_range_option::InRangeOption;

#[derive(Debug)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShortcutData {
    down_arc: InRangeOption<EdgeId>,
    up_arc: EdgeId
}

impl ShortcutData {
    pub fn new(source: ShortcutSource) -> ShortcutData {
        match source {
            ShortcutSource::Shortcut(down, up) => ShortcutData { down_arc: InRangeOption::new(Some(down)), up_arc: up },
            ShortcutSource::OriginalEdge(edge) => ShortcutData { down_arc: InRangeOption::new(None), up_arc: edge },
        }
    }

    pub fn source(&self) -> ShortcutSource {
        match self.down_arc.value() {
            Some(down_shortcut_id) => ShortcutSource::Shortcut(down_shortcut_id, self.up_arc),
            None => ShortcutSource::OriginalEdge(self.up_arc),
        }
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).evaluate(departure, shortcut_graph)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).evaluate(departure)
            },
        }
    }

    pub fn ipp_iter<'a>(&self, range: WrappingRange<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> ShortcutSourceIter<'a> {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                ShortcutSourceIter::Shortcut(Linked::new(down_shortcut_id, self.up_arc).ipp_iter(range, shortcut_graph))
            },
            None => {
                ShortcutSourceIter::OriginalEdge(shortcut_graph.original_graph().travel_time_function(self.up_arc).ipp_iter(range))
            },
        }
    }

    pub fn bounds(&self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).bounds(shortcut_graph)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).bounds()
            },
        }
    }
}

pub enum ShortcutSourceIter<'a> {
    Shortcut(linked::Iter<'a>),
    OriginalEdge(piecewise_linear_function::Iter<'a>),
}

impl<'a> Iterator for ShortcutSourceIter<'a> {
    type Item = (Timestamp, Weight);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            &mut ShortcutSourceIter::Shortcut(ref mut iter) => iter.next(),
            &mut ShortcutSourceIter::OriginalEdge(ref mut iter) => iter.next(),
        }
    }
}
