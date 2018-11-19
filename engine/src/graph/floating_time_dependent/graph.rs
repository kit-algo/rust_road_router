use super::*;
use crate::graph::Graph as GraphTrait;

type IPPIndex = u32;

#[derive(Debug, Clone)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipps: Vec<TTFPoint>,
}

impl Graph {
    pub fn new(first_out: Vec<EdgeId>,
               head: Vec<NodeId>,
               first_ipp_of_arc: Vec<IPPIndex>,
               ipps: Vec<TTFPoint>) -> Graph {
        Graph { first_out, head, first_ipp_of_arc, ipps }
    }

    pub fn travel_time_function(&self, edge_id: EdgeId) -> PiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PiecewiseLinearFunction::new(&self.ipps[self.first_ipp_of_arc[edge_id] as usize .. self.first_ipp_of_arc[edge_id + 1] as usize])
    }

    pub fn neighbor_and_edge_id_iter(&self, node: NodeId) -> impl Iterator<Item = (&NodeId, EdgeId)> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().zip(self.neighbor_edge_indices(node))
    }

    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }
}

impl GraphTrait for Graph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head.len()
    }
}

impl<'a> LinkIterGraph<'a> for Graph {
    type Iter = std::iter::Map<std::slice::Iter<'a, NodeId>, fn(&NodeId)->Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().map(|&head| Link { node: head, weight: 0 })
    }
}

impl RandomLinkAccessGraph for Graph {
    fn link(&self, edge_id: EdgeId) -> Link {
        Link { node: self.head[edge_id as usize], weight: 0 }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out[from as usize] as usize;
        self.neighbor_iter(from).enumerate().find(|&(_, Link { node, .. })| node == to).map(|(i, _)| (first_out + i) as EdgeId )
    }

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }
}
