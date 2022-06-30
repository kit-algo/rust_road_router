//! CCH query based on elimination tree

use super::*;
pub mod nearest_neighbor;
pub mod stepped_elimination_tree;
use stepped_elimination_tree::EliminationTreeWalk;

pub struct Server<Customized> {
    customized: Customized,
    fw_distances: Vec<Weight>,
    bw_distances: Vec<Weight>,
    fw_parents: Vec<(NodeId, EdgeId)>,
    bw_parents: Vec<(NodeId, EdgeId)>,
    meeting_node: NodeId,
    relaxed_edges: usize,
    walked_nodes: usize,
}

impl<C: Customized> Server<C> {
    pub fn new(customized: C) -> Self {
        let n = customized.forward_graph().num_nodes();
        let m = customized.forward_graph().num_arcs();
        Server {
            customized,
            fw_distances: vec![INFINITY; n],
            bw_distances: vec![INFINITY; n],
            fw_parents: vec![(n as NodeId, m as EdgeId); n],
            bw_parents: vec![(n as NodeId, m as EdgeId); n],
            meeting_node: 0,
            relaxed_edges: 0,
            walked_nodes: 0,
        }
    }

    // Update the metric using a new customization result
    pub fn update(&mut self, mut customized: C) {
        std::mem::swap(&mut self.customized, &mut customized);
    }

    pub fn customized(&self) -> &C {
        &self.customized
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.walked_nodes = 0;
        self.relaxed_edges = 0;
        let from = self.customized.cch().node_order().rank(from);
        let to = self.customized.cch().node_order().rank(to);

        let fw_graph = self.customized.forward_graph();
        let bw_graph = self.customized.backward_graph();

        // initialize
        let mut tentative_distance = INFINITY;
        self.meeting_node = 0;
        let mut fw_walk = EliminationTreeWalk::query_with_resetted(
            &fw_graph,
            self.customized.cch().elimination_tree(),
            &mut self.fw_distances,
            &mut self.fw_parents,
            from,
        );
        let mut bw_walk = EliminationTreeWalk::query_with_resetted(
            &bw_graph,
            self.customized.cch().elimination_tree(),
            &mut self.bw_distances,
            &mut self.bw_parents,
            to,
        );

        // maybe split loop to exploit that once we use the "both nodes"-case, we will always use that?
        loop {
            match (fw_walk.peek(), bw_walk.peek()) {
                (Some(fw_node), Some(bw_node)) if fw_node < bw_node => {
                    self.walked_nodes += 1;
                    fw_walk.next();
                    fw_walk.reset_distance(fw_node);
                }
                (Some(fw_node), Some(bw_node)) if fw_node > bw_node => {
                    self.walked_nodes += 1;
                    bw_walk.next();
                    bw_walk.reset_distance(bw_node);
                }
                (Some(node), Some(_node)) => {
                    debug_assert_eq!(node, _node);
                    self.walked_nodes += 1;
                    if fw_walk.tentative_distance(node) < tentative_distance {
                        fw_walk.next();
                    } else {
                        fw_walk.skip_next();
                    }
                    if bw_walk.tentative_distance(node) < tentative_distance {
                        bw_walk.next();
                    } else {
                        bw_walk.skip_next();
                    }
                    let dist = fw_walk.tentative_distance(node) + bw_walk.tentative_distance(node);
                    if dist < tentative_distance {
                        tentative_distance = dist;
                        self.meeting_node = node;
                    }
                    fw_walk.reset_distance(node);
                    bw_walk.reset_distance(node);
                }
                // the (Some,None) case can only happen when the nodes
                // share no common ancestores in the elimination tree
                // thus, there will be no path but we will still need
                // to walk the path up to reset all distances
                (Some(fw_node), None) => {
                    self.walked_nodes += 1;
                    fw_walk.next();
                    fw_walk.reset_distance(fw_node);
                }
                (None, Some(bw_node)) => {
                    self.walked_nodes += 1;
                    bw_walk.next();
                    bw_walk.reset_distance(bw_node);
                }
                (None, None) => break,
            }
        }

        self.relaxed_edges = fw_walk.num_relaxed_edges() + bw_walk.num_relaxed_edges();

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    fn path(&mut self, query: Query) -> Vec<NodeId> {
        let from = self.customized.cch().node_order().rank(query.from);
        let to = self.customized.cch().node_order().rank(query.to);

        let mut node = self.meeting_node;
        while node != from {
            let (parent, edge) = self.fw_parents[node as usize];
            self.bw_parents[parent as usize] = (node, edge);
            node = parent;
        }

        // unpack shortcuts so that parant pointers already point along the completely unpacked path
        Self::unpack_path(to, from, &self.customized, &mut self.bw_parents);

        let mut path = vec![from];

        while *path.last().unwrap() != to {
            path.push(self.bw_parents[*path.last().unwrap() as usize].0);
        }

        for node in &mut path {
            *node = self.customized.cch().node_order().node(*node);
        }

        path
    }

    /// Unpack path from a start node (the meeting node of the CCH query), so that parent pointers point along the unpacked path.
    fn unpack_path(origin: NodeId, target: NodeId, customzied: &C, parents: &mut [(NodeId, EdgeId)]) {
        let mut current = target;
        while current != origin {
            let (pred, edge) = parents[current as usize];

            let unpacked = if pred > current {
                customzied.unpack_outgoing(EdgeIdT(edge))
            } else {
                customzied.unpack_incoming(EdgeIdT(edge))
            };
            if let Some((EdgeIdT(down), EdgeIdT(up), NodeIdT(middle))) = unpacked {
                parents[current as usize] = (middle, down);
                parents[middle as usize] = (pred, up);
            } else {
                current = pred;
            }
        }
    }
}

pub struct PathServerWrapper<'s, C>(&'s mut Server<C>, Query);

impl<'s, C: Customized> PathServerWrapper<'s, C> {
    pub fn num_nodes_in_searchspace(&self) -> usize {
        self.0.walked_nodes
    }
    pub fn num_relaxed_edges(&self) -> usize {
        self.0.relaxed_edges
    }
}

impl<'s, C: Customized> PathServer for PathServerWrapper<'s, C> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<C: Customized> QueryServer for Server<C> {
    type P<'s> = PathServerWrapper<'s, C> where Self: 's;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
