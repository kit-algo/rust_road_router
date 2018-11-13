use super::*;
use crate::in_range_option::InRangeOption;

#[derive(Debug, Clone, Copy)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

impl ShortcutSource {
    pub(super) fn evaluate<F>(&self, t: Timestamp, shortcut_graph: &ShortcutGraph, f: &mut F) -> FlWeight
        where F: (FnMut(bool, EdgeId, Timestamp) -> bool)
    {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                if !f(false, down, t) { return FlWeight::new(f64::from(INFINITY)); }
                let first_val = shortcut_graph.get_incoming(down).evaluate(t, shortcut_graph, f);
                let t_mid = t + first_val;
                if !f(true, up, t_mid) { return FlWeight::new(f64::from(INFINITY)); }
                first_val + shortcut_graph.get_outgoing(up).evaluate(t_mid, shortcut_graph, f)
            },
            ShortcutSource::OriginalEdge(edge) => shortcut_graph.original_graph().travel_time_function(edge).evaluate(t),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShortcutSourceData {
    down_arc: InRangeOption<EdgeId>,
    up_arc: EdgeId
}

impl From<ShortcutSource> for ShortcutSourceData {
    fn from(source: ShortcutSource) -> Self {
        match source {
            ShortcutSource::Shortcut(down, up) => ShortcutSourceData { down_arc: InRangeOption::new(Some(down)), up_arc: up },
            ShortcutSource::OriginalEdge(edge) => ShortcutSourceData { down_arc: InRangeOption::new(None), up_arc: edge },
        }
    }
}

impl From<ShortcutSourceData> for ShortcutSource {
    fn from(data: ShortcutSourceData) -> Self {
        match data.down_arc.value() {
            Some(down_shortcut_id) => {
                ShortcutSource::Shortcut(down_shortcut_id, data.up_arc)
            },
            None => {
                ShortcutSource::OriginalEdge(data.up_arc)
            },
        }
    }
}
