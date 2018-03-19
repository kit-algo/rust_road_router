use super::*;

type IPPIndex = u32;

#[derive(Debug)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipp_departure_time: Vec<Timestamp>,
    ipp_travel_time: Vec<Weight>,
    period: Timestamp
}

impl Graph {
    pub fn travel_time_function(&self, edge_id: EdgeId) -> PiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PiecewiseLinearFunction::new(
            &self.ipp_departure_time[self.first_ipp_of_arc[edge_id] as usize .. self.first_ipp_of_arc[edge_id + 1] as usize],
            &self.ipp_travel_time[self.first_ipp_of_arc[edge_id] as usize .. self.first_ipp_of_arc[edge_id + 1] as usize],
            self.period)
    }

    pub fn period(&self) -> Timestamp {
        self.period
    }

    pub fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }

    pub fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
        Range { start: range.start as usize, end: range.end as usize }
    }

    pub fn neighbor_iter(&self, node: NodeId) -> std::iter::Cloned<std::slice::Iter<u32>> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned()
    }
}
