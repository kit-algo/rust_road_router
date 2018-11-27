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
                debug_assert!(first_val >= FlWeight::zero());
                let t_mid = t + first_val;
                if !f(true, up, t_mid) { return FlWeight::new(f64::from(INFINITY)); }
                let second_val = shortcut_graph.get_outgoing(up).evaluate(t_mid, shortcut_graph, f);
                debug_assert!(second_val >= FlWeight::zero());
                first_val + second_val
            },
            ShortcutSource::OriginalEdge(edge) => {
                let res = shortcut_graph.original_graph().travel_time_function(edge).evaluate(t);
                debug_assert!(res >= FlWeight::zero());
                res
            },
        }
    }

    pub(super) fn unpack_at(&self, t: Timestamp, shortcut_graph: &ShortcutGraph) -> Vec<(NodeId, Timestamp)> {
        match *self {
            ShortcutSource::Shortcut(down, up) => {
                let first_val = shortcut_graph.get_incoming(down).evaluate(t, shortcut_graph, &mut |_,_,_| true);
                let t_mid = t + first_val;
                let mut res = shortcut_graph.get_incoming(down).unpack_at(t, shortcut_graph);
                let (_, t_last) = res.pop().unwrap();
                debug_assert!(t_last.fuzzy_eq(t_mid), "expected {:?} got {:?} on {:?}", t_last, t_mid, self);
                res.append(&mut shortcut_graph.get_outgoing(up).unpack_at(t_mid, shortcut_graph));
                res
            },
            ShortcutSource::OriginalEdge(edge) => {
                let arr = t + shortcut_graph.original_graph().travel_time_function(edge).evaluate(t);
                let mut res = Vec::new();
                let head = shortcut_graph.original_graph().head()[edge as usize];
                let tail = match shortcut_graph.original_graph().first_out().binary_search(&edge) {
                    Ok(i) => i,
                    Err(i) => i - 1
                } as NodeId;
                debug_assert!(arr >= t);
                res.push((tail, t));
                res.push((head, arr));
                res
            },
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
