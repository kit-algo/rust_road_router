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

    pub fn evaluate(&self, departure: Timestamp) -> Weight {
        // let first_edge = shortcut_graph.get(self.first);
        // let second_edge = shortcut_graph.get(self.second);
        // second_edge.evaluate(departure + first_edge.evaluate(departure))
        unimplemented!();
    }

    pub fn next_ipp_after(&self, time: Timestamp) -> Option<Timestamp> {
        unimplemented!();
    }
}
