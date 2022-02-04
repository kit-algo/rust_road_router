//! CCH query based on elimination tree

use super::*;
pub mod stepped_elimination_tree;
use stepped_elimination_tree::EliminationTreeWalk;

pub struct Server<CCH, CCHB> {
    customized: Customized<CCH, CCHB>,
    fw_distances: Vec<Weight>,
    bw_distances: Vec<Weight>,
    unpacking_distances: Vec<Weight>,
    fw_parents: Vec<(NodeId, EdgeId)>,
    bw_parents: Vec<(NodeId, EdgeId)>,
    meeting_node: NodeId,
}

impl<'a, CCH: CCHT, CCHB: std::borrow::Borrow<CCH>> Server<CCH, CCHB> {
    pub fn new(customized: Customized<CCH, CCHB>) -> Self {
        let n = customized.forward_graph().num_nodes();
        let m = customized.forward_graph().num_arcs();
        Server {
            customized,
            fw_distances: vec![INFINITY; n],
            bw_distances: vec![INFINITY; n],
            unpacking_distances: vec![INFINITY; n],
            fw_parents: vec![(n as NodeId, m as EdgeId); n],
            bw_parents: vec![(n as NodeId, m as EdgeId); n],
            meeting_node: 0,
        }
    }

    // Update the metric using a new customization result
    pub fn update(&mut self, mut customized: Customized<CCH, CCHB>) {
        std::mem::swap(&mut self.customized, &mut customized);
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.customized.cch.borrow().node_order().rank(from);
        let to = self.customized.cch.borrow().node_order().rank(to);

        let fw_graph = self.customized.forward_graph();
        let bw_graph = self.customized.backward_graph();

        // initialize
        let mut tentative_distance = INFINITY;
        self.meeting_node = 0;
        let mut fw_walk = EliminationTreeWalk::query_with_resetted(
            &fw_graph,
            self.customized.cch.borrow().elimination_tree(),
            &mut self.fw_distances,
            &mut self.fw_parents,
            from,
        );
        let mut bw_walk = EliminationTreeWalk::query_with_resetted(
            &bw_graph,
            self.customized.cch.borrow().elimination_tree(),
            &mut self.bw_distances,
            &mut self.bw_parents,
            to,
        );

        // maybe split loop to exploit that once we use the "both nodes"-case, we will always use that?
        loop {
            match (fw_walk.peek(), bw_walk.peek()) {
                (Some(fw_node), Some(bw_node)) if fw_node < bw_node => {
                    fw_walk.next();
                    fw_walk.reset_distance(fw_node);
                }
                (Some(fw_node), Some(bw_node)) if fw_node > bw_node => {
                    bw_walk.next();
                    bw_walk.reset_distance(bw_node);
                }
                (Some(node), Some(_node)) => {
                    debug_assert_eq!(node, _node);
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
                    fw_walk.next();
                    fw_walk.reset_distance(fw_node);
                }
                (None, Some(bw_node)) => {
                    bw_walk.next();
                    bw_walk.reset_distance(bw_node);
                }
                (None, None) => break,
            }
        }

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    fn path(&mut self, query: Query) -> Vec<NodeId> {
        let from = self.customized.cch.borrow().node_order().rank(query.from);
        let to = self.customized.cch.borrow().node_order().rank(query.to);
        let fw_graph = self.customized.forward_graph();
        let bw_graph = self.customized.backward_graph();

        let mut node = self.meeting_node;
        while node != to {
            let parent = self.bw_parents[node as usize].0;
            self.fw_parents[parent as usize].0 = node;
            node = parent;
        }

        self.unpacking_distances[to as usize] = 0;
        debug_assert_eq!(node, to);
        while node != self.meeting_node {
            let parent = self.fw_parents[node as usize].0;
            self.unpacking_distances[parent as usize] =
                self.unpacking_distances[node as usize] + bw_graph.weight()[bw_graph.edge_indices(node, parent).next().unwrap().0 as usize];
            node = parent;
        }

        debug_assert_eq!(node, self.meeting_node);
        while node != from {
            let parent = self.fw_parents[node as usize].0;
            self.bw_parents[parent as usize].0 = node;
            self.unpacking_distances[parent as usize] =
                self.unpacking_distances[node as usize] + fw_graph.weight()[fw_graph.edge_indices(parent, node).next().unwrap().0 as usize];
            node = parent;
        }

        // unpack shortcuts so that parant pointers already point along the completely unpacked path
        Self::unpack_path(
            to,
            from,
            self.customized.cch.borrow(),
            bw_graph.weight(),
            fw_graph.weight(),
            &mut self.unpacking_distances,
            &mut self.bw_parents,
        );

        let mut path = vec![from];

        while *path.last().unwrap() != to {
            path.push(self.bw_parents[*path.last().unwrap() as usize].0);
        }

        for node in &mut path {
            *node = self.customized.cch.borrow().node_order().node(*node);
        }

        path
    }

    /// Unpack path from a start node (the meeting node of the CCH query), so that parent pointers point along the unpacked path.
    fn unpack_path(
        origin: NodeId,
        target: NodeId,
        cch: &CCH,
        weights: &[Weight],
        other_weights: &[Weight],
        distances: &mut [Weight],
        parents: &mut [(NodeId, EdgeId)],
    ) {
        let mut current = target;
        while current != origin {
            let pred = parents[current as usize].0;
            let weight = distances[current as usize] - distances[pred as usize];

            let unpacked = cch.unpack_arc(current, pred, weight, other_weights, weights);
            if let Some((middle, _first_weight, second_weight)) = unpacked {
                parents[current as usize].0 = middle;
                parents[middle as usize].0 = pred;
                distances[middle as usize] = distances[pred as usize] + second_weight;
            } else {
                current = pred;
            }
        }
    }
}

pub struct PathServerWrapper<'s, CCH, CCHB>(&'s mut Server<CCH, CCHB>, Query);

impl<'s, CCH: CCHT, CCHB: std::borrow::Borrow<CCH>> PathServer for PathServerWrapper<'s, CCH, CCHB> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<'a, CCH: CCHT, CCHB: std::borrow::Borrow<CCH>> QueryServer for Server<CCH, CCHB> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, CCH, CCHB>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
