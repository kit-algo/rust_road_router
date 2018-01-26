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

    pub fn evaluate(&self, departure: Timestamp, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) -> Weight {
        let first_edge = shortcut_graph.get(self.first);
        let second_edge = shortcut_graph.get(self.second);
        second_edge.evaluate(departure + first_edge.evaluate(departure, original_graph, shortcut_graph), original_graph, shortcut_graph)
    }

    pub fn next_ipp_greater_eq(&self, time: Timestamp, original_graph: &TDGraph, shortcut_graph: &ShortcutGraph) -> Option<Timestamp> {
        let first_edge = shortcut_graph.get(self.first);
        let second_edge = shortcut_graph.get(self.second);
        let first_edge_next_ipp = first_edge.next_ipp_greater_eq(time, original_graph, shortcut_graph);
        let first_edge_next_ipp_at = first_edge_next_ipp.unwrap_or(original_graph.period()); // TODO end of period, need range
        let first_edge_current_value = first_edge.evaluate(time, original_graph, shortcut_graph);
        let first_edge_next_ipp_value = first_edge.evaluate(first_edge_next_ipp_at, original_graph, shortcut_graph);

        let second_edge_next_ipp = second_edge.next_ipp_greater_eq(time + first_edge_current_value, original_graph, shortcut_graph);
        match second_edge_next_ipp {
            Some(second_edge_next_ipp_at) => {
                if second_edge_next_ipp_at < first_edge_next_ipp_at + first_edge_next_ipp_value {
                    Some(invert((time, time + first_edge_current_value), (first_edge_next_ipp_at, first_edge_next_ipp_at + first_edge_next_ipp_value), second_edge_next_ipp_at, original_graph.period()))
                } else {
                    first_edge_next_ipp
                }
            },
            None => {
                // wrapping
                match second_edge.next_ipp_greater_eq(0, original_graph, shortcut_graph) {
                    Some(second_edge_next_ipp_at) => {
                        if second_edge_next_ipp_at < first_edge_next_ipp_at + first_edge_next_ipp_value {
                            Some(invert((time, time + first_edge_current_value), (first_edge_next_ipp_at, first_edge_next_ipp_at + first_edge_next_ipp_value), second_edge_next_ipp_at, original_graph.period()))
                        } else {
                            first_edge_next_ipp
                        }
                    },
                    None => {
                        first_edge_next_ipp
                    },
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
