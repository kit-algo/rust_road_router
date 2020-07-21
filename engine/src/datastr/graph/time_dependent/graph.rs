use super::*;
use crate::datastr::graph::Graph as GraphTrait;

type IPPIndex = u32;

/// Container for basic TD-Graph data.
#[derive(Debug, Clone)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipp_departure_time: Vec<Timestamp>,
    ipp_travel_time: Vec<Weight>,
}

impl Graph {
    /// Construct graph from raw data.
    pub fn new(
        first_out: Vec<EdgeId>,
        head: Vec<NodeId>,
        mut first_ipp_of_arc: Vec<IPPIndex>,
        ipp_departure_time: Vec<Timestamp>,
        ipp_travel_time: Vec<Weight>,
    ) -> Self {
        let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
        let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

        let mut added = 0;

        for i in 0..head.len() {
            let range = first_ipp_of_arc[i] as usize..first_ipp_of_arc[i + 1] as usize;
            assert_ne!(range.start, range.end);

            first_ipp_of_arc[i] += added;

            if range.end - range.start > 1 {
                if ipp_departure_time[range.start] != 0 {
                    new_ipp_departure_time.push(0);
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
                new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
                new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
                if *new_ipp_departure_time.last().unwrap() != period() {
                    new_ipp_departure_time.push(period());
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
            } else {
                new_ipp_departure_time.push(0);
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
            }
        }
        first_ipp_of_arc[head.len()] += added;

        Self {
            first_out,
            head,
            first_ipp_of_arc,
            ipp_departure_time: new_ipp_departure_time,
            ipp_travel_time: new_ipp_travel_time,
        }
    }

    /// Borrow an individual travel time function.
    pub fn travel_time_function(&self, edge_id: EdgeId) -> PiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PiecewiseLinearFunction::new(
            &self.ipp_departure_time[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize],
            &self.ipp_travel_time[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize],
        )
    }

    /// Iterator over neighbors and corresponding edge ids.
    pub fn neighbor_and_edge_id_iter(&self, node: NodeId) -> impl Iterator<Item = (NodeId, EdgeId)> + '_ {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned().zip(self.neighbor_edge_indices(node))
    }

    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }

    pub fn permute_node_ids(&self, order: &NodeOrder) -> Self {
        let mut first_out: Vec<EdgeId> = Vec::with_capacity(self.num_nodes() + 1);
        first_out.push(0);
        let mut head = Vec::with_capacity(self.num_arcs());
        let mut first_ipp_of_arc = Vec::<IPPIndex>::with_capacity(self.num_arcs());
        first_ipp_of_arc.push(0);
        let mut ipp_departure_time = Vec::<Timestamp>::with_capacity(self.ipp_departure_time.len());
        let mut ipp_travel_time = Vec::<Weight>::with_capacity(self.ipp_travel_time.len());

        for &node in order.order() {
            first_out.push(first_out.last().unwrap() + self.degree(node) as NodeId);
            let mut links = self.neighbor_and_edge_id_iter(node).collect::<Vec<_>>();
            links.sort_unstable_by_key(|&(head, _)| order.rank(head));

            for (h, e) in links {
                head.push(order.rank(h));
                let ipp_range = self.first_ipp_of_arc[e as usize] as usize..self.first_ipp_of_arc[e as usize + 1] as usize;
                first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + (ipp_range.end - ipp_range.start) as IPPIndex);
                ipp_departure_time.extend_from_slice(&self.ipp_departure_time[ipp_range.clone()]);
                ipp_travel_time.extend_from_slice(&self.ipp_travel_time[ipp_range]);
            }
        }

        Graph {
            first_out,
            head,
            first_ipp_of_arc,
            ipp_departure_time,
            ipp_travel_time,
        }
    }

    /// Total number of interpolation points
    pub fn num_ipps(&self) -> usize {
        self.ipp_departure_time.len()
    }

    /// Number of edges with constant PLF
    pub fn num_constant(&self) -> usize {
        self.first_ipp_of_arc
            .windows(2)
            .map(|firsts| firsts[1] - firsts[0])
            .filter(|&deg| deg == 1)
            .count()
    }
}

impl GraphTrait for Graph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head.len()
    }

    fn degree(&self, node: NodeId) -> usize {
        let node = node as usize;
        (self.first_out[node + 1] - self.first_out[node]) as usize
    }
}

impl<'a> LinkIterGraph<'a> for Graph {
    type Iter = std::iter::Map<std::slice::Iter<'a, NodeId>, fn(&NodeId) -> Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().map(|&head| Link { node: head, weight: 0 })
    }
}

impl RandomLinkAccessGraph for Graph {
    fn link(&self, edge_id: EdgeId) -> Link {
        Link {
            node: self.head[edge_id as usize],
            weight: 0,
        }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out[from as usize] as usize;
        self.neighbor_iter(from)
            .enumerate()
            .find(|&(_, Link { node, .. })| node == to)
            .map(|(i, _)| (first_out + i) as EdgeId)
    }

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }
}

impl crate::algo::dijkstra::generic_dijkstra::Lnk for (NodeId, EdgeId) {
    fn head(&self) -> NodeId {
        self.0
    }
}

impl<'a> LinkIterable<'a, (NodeId, EdgeId)> for Graph {
    type Iter = std::iter::Zip<std::iter::Cloned<std::slice::Iter<'a, NodeId>>, std::ops::Range<EdgeId>>;
    #[inline(always)]
    fn link_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned().zip(self.neighbor_edge_indices(node))
    }
}
