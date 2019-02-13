use super::*;
use crate::in_range_option::InRangeOption;

#[derive(Debug, Clone, Copy)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

impl ShortcutSource {
    pub(super) fn evaluate<F>(&self, t: Timestamp, customized_graph: &CustomizedGraph, f: &mut F) -> FlWeight
        where F: (FnMut(bool, EdgeId, Timestamp) -> bool)
    {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                if !f(false, down, t) { return FlWeight::INFINITY; }
                let first_val = customized_graph.incoming.evaluate(down, t, customized_graph, f);
                debug_assert!(first_val >= FlWeight::zero());
                let t_mid = t + first_val;
                if !f(true, up, t_mid) { return FlWeight::INFINITY; }
                let second_val = customized_graph.outgoing.evaluate(up, t_mid, customized_graph, f);
                debug_assert!(second_val >= FlWeight::zero());
                first_val + second_val
            },
            ShortcutSource::OriginalEdge(edge) => {
                let res = customized_graph.original_graph.travel_time_function(edge).evaluate(t);
                debug_assert!(res >= FlWeight::zero());
                res
            },
        }
    }

    pub(super) fn unpack_at(&self, t: Timestamp, customized_graph: &CustomizedGraph, result: &mut Vec<(EdgeId, Timestamp)>) {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                customized_graph.incoming.unpack_at(down, t, customized_graph, result);
                let t_mid = result.last().unwrap().1;
                customized_graph.outgoing.unpack_at(up, t_mid, customized_graph, result);
            },
            ShortcutSource::OriginalEdge(edge) => {
                let arr = t + customized_graph.original_graph.travel_time_function(edge).evaluate(t);
                result.push((edge, arr))
            },
        }
    }

    pub(super) fn exact_ttf_for(&self, start: Timestamp, end: Timestamp, shortcut_graph: &ShortcutGraph) -> Vec<TTFPoint> {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                let first = shortcut_graph.get_incoming(down).exact_ttf_for(start, end, shortcut_graph);
                let second_start = first.first().unwrap().at + first.first().unwrap().val;
                let second_end = first.last().unwrap().at + first.last().unwrap().val;
                let second = shortcut_graph.get_outgoing(up).exact_ttf_for(second_start, second_end, shortcut_graph);
                PiecewiseLinearFunction::link_partials(first, second)
            }
            ShortcutSource::OriginalEdge(edge) => {
                shortcut_graph.original_graph().travel_time_function(edge).copy_range(start, end)
            }
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

// TODO Default should be valid
impl Default for ShortcutSourceData {
    fn default() -> Self {
        Self {
            down_arc: InRangeOption::new(None),
            up_arc: std::u32::MAX,
        }
    }
}
