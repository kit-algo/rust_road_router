use super::*;
use crate::in_range_option::InRangeOption;

#[derive(Debug, Clone, Copy)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

impl ShortcutSource {
    pub(super) fn eval(&self, t: Timestamp, shortcut_graph: &ShortcutGraph) -> FlWeight {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                let first_val = shortcut_graph.get_incoming(down).eval(t, shortcut_graph);
                let t_mid = t + first_val;
                let (_, t_sec) = t_mid.split_of_period();
                first_val + shortcut_graph.get_outgoing(up).eval(t_sec, shortcut_graph)
            },
            ShortcutSource::OriginalEdge(edge) => shortcut_graph.original_graph().travel_time_function(edge).eval(t),
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
