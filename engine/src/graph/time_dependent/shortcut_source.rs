use super::*;
use super::linked::*;
use in_range_option::InRangeOption;

#[derive(Debug)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

#[derive(Debug, Clone, Copy)]
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

    pub fn evaluate(&self, departure: Timestamp) -> Weight {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).evaluate(departure)
            },
            None => {
                // TODO get data from actual graph
                // graph.get(self.up_arc).evaluate(departure)
                unimplemented!()
            },
        }
    }

    pub fn next_ipp_greater_eq(&self, time: Timestamp) -> Option<Timestamp> {
        match self.down_arc.value() {
            Some(down_shortcut_id) => {
                Linked::new(down_shortcut_id, self.up_arc).next_ipp_greater_eq(time)
            },
            None => {
                // TODO get data from actual graph
                // graph.get(self.up_arc).next_ipp_greater_eq(time)
                unimplemented!()
            },
        }
    }
}
