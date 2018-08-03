use super::*;
use rank_select_map::BitVec;
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
                Linked::new(shortcut_graph.get_incoming(down_shortcut_id), shortcut_graph.get_outgoing(self.up_arc)).evaluate(departure, shortcut_graph)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).evaluate(departure)
            },
        }
    }

    pub fn bounds_for(self, range: &Range<Timestamp>, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(shortcut_graph.get_incoming(down_shortcut_id), shortcut_graph.get_outgoing(self.up_arc)).bounds_for(range)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).bounds_for(range)
            },
        }
    }

    pub fn unpack(self, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                if !unpacked_shortcuts.get(down_shortcut_id as usize * 2) {
                    unpacked_shortcuts.set(down_shortcut_id as usize * 2);
                    shortcut_graph.get_incoming(down_shortcut_id).unpack(shortcut_graph, unpacked_shortcuts, original_edges);
                }
                if !unpacked_shortcuts.get(self.up_arc as usize * 2 + 1) {
                    unpacked_shortcuts.set(self.up_arc as usize * 2 + 1);
                    shortcut_graph.get_outgoing(self.up_arc).unpack(shortcut_graph, unpacked_shortcuts, original_edges);
                }
            },
            None => {
                original_edges.set(self.up_arc as usize);
            },
        }
    }

    pub fn debug_to_s(self, shortcut_graph: &ShortcutGraph, indent: usize) -> String {
        println!("{:?}", self.source());
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(shortcut_graph.get_incoming(down_shortcut_id), shortcut_graph.get_outgoing(self.up_arc)).debug_to_s(shortcut_graph, indent)
            },
            None => {
                shortcut_graph.original_graph().travel_time_function(self.up_arc).debug_to_s(indent)
            },
        }
    }
}
