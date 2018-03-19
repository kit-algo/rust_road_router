use super::*;
use graph::Graph as GraphTrait;

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
    pub fn new(first_out: Vec<EdgeId>,
               head: Vec<NodeId>,
               first_ipp_of_arc: Vec<IPPIndex>,
               ipp_departure_time: Vec<Timestamp>,
               ipp_travel_time: Vec<Weight>,
               period: Timestamp) -> Graph {
        Graph { first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time, period }
    }

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
}

impl GraphTrait for Graph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }
}

impl<'a> LinkIterGraph<'a> for Graph {
    type Iter = std::iter::Map<std::slice::Iter<'a, NodeId>, fn(&NodeId)->Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().map(|&head| Link { node: head, weight: 0 })
    }
}
