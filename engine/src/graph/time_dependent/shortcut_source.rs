use super::*;
use in_range_option::InRangeOption;

#[derive(Debug, Clone, Copy)]
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

    pub fn source(self) -> ShortcutSource {
        match self.down_arc.value() {
            Some(down_shortcut_id) => ShortcutSource::Shortcut(down_shortcut_id, self.up_arc),
            None => ShortcutSource::OriginalEdge(self.up_arc),
        }
    }

    pub fn evaluate(self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).evaluate(departure, shortcut_graph)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).evaluate(departure)
            },
        }
    }

    pub(super) fn seg_iter<'a>(self, range: WrappingRange, shortcut_graph: &'a ShortcutGraph) -> ShortcutSourceSegmentIter<'a, impl Iterator<Item = PLFSeg> + 'a> {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                ShortcutSourceSegmentIter::Shortcut(Linked::new(down_shortcut_id, self.up_arc).seg_iter(range, shortcut_graph))
            },
            None => {
                ShortcutSourceSegmentIter::OriginalEdge(shortcut_graph.original_graph().travel_time_function(self.up_arc).seg_iter(range))
            },
        }
    }

    pub fn bounds(self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).bounds(shortcut_graph)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).bounds()
            },
        }
    }

    pub fn debug_to_s(self, shortcut_graph: &ShortcutGraph, indent: usize) -> String {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).debug_to_s(shortcut_graph, indent)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).debug_to_s(indent)
            },
        }
    }
}

pub(super) enum ShortcutSourceSegmentIter<'a, PLFIter: Iterator<Item = PLFSeg>> {
    Shortcut(linked::SegmentIter<'a>), // TODO move Box here?
    OriginalEdge(PLFIter),
}

impl<'a, PLFIter: Iterator<Item = PLFSeg>> Iterator for ShortcutSourceSegmentIter<'a, PLFIter> {
    type Item = TTFSeg;

    fn next(&mut self) -> Option<Self::Item> {
        // println!("shortcut src next");
        match *self {
            ShortcutSourceSegmentIter::Shortcut(ref mut iter) => iter.next(),
            ShortcutSourceSegmentIter::OriginalEdge(ref mut iter) => iter.next().map(|seg| seg.into_ttfseg()),
        }
    }
}
