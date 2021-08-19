//! CCH query based on elimination tree

use super::*;
pub mod stepped_elimination_tree;
use crate::datastr::timestamped_vector::TimestampedVector;
use stepped_elimination_tree::EliminationTreeWalk;

#[derive(Debug)]
pub struct Server<'a, CCH> {
    customized: Customized<CCH, &'a CCH>,
    fw_distances: TimestampedVector<Weight>,
    bw_distances: TimestampedVector<Weight>,
    fw_parents: Vec<NodeId>,
    bw_parents: Vec<NodeId>,
    meeting_node: NodeId,
}

impl<'a, CCH: CCHT> Server<'a, CCH> {
    pub fn new(customized: Customized<CCH, &'a CCH>) -> Self {
        let n = customized.forward_graph().num_nodes();
        Server {
            customized,
            fw_distances: TimestampedVector::new(n, INFINITY),
            bw_distances: TimestampedVector::new(n, INFINITY),
            fw_parents: vec![n as NodeId; n],
            bw_parents: vec![n as NodeId; n],
            meeting_node: 0,
        }
    }

    // Update the metric using a new customization result
    pub fn update(&mut self, mut customized: Customized<CCH, &'a CCH>) {
        std::mem::swap(&mut self.customized, &mut customized);
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.customized.cch.node_order().rank(from);
        let to = self.customized.cch.node_order().rank(to);

        let fw_graph = self.customized.forward_graph();
        let bw_graph = self.customized.backward_graph();

        // initialize
        let mut tentative_distance = INFINITY;
        self.meeting_node = 0;
        let mut fw_walk = EliminationTreeWalk::query(
            &fw_graph,
            self.customized.cch.elimination_tree(),
            &mut self.fw_distances,
            &mut self.fw_parents,
            from,
        );
        let mut bw_walk = EliminationTreeWalk::query(
            &bw_graph,
            self.customized.cch.elimination_tree(),
            &mut self.bw_distances,
            &mut self.bw_parents,
            to,
        );

        loop {
            match (fw_walk.peek(), bw_walk.peek()) {
                (Some(fw_node), Some(bw_node)) if fw_node < bw_node => {
                    fw_walk.next();
                }
                (Some(fw_node), Some(bw_node)) if fw_node > bw_node => {
                    bw_walk.next();
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
                }
                // the (Some,None) case can only happen when the nodes
                // share no common ancestores in the elimination tree
                // thus, there will be no path
                (Some(_), None) => return None,
                (None, Some(_)) => return None,
                (None, None) => break,
            }
        }

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    fn path(&mut self, query: Query) -> Vec<NodeId> {
        let from = self.customized.cch.node_order().rank(query.from);
        let to = self.customized.cch.node_order().rank(query.to);

        // unpack shortcuts so that parant pointers already point along the completely unpacked path
        Self::unpack_path(
            from,
            self.meeting_node,
            true,
            self.customized.cch,
            self.customized.forward_graph().weight(),
            self.customized.backward_graph().weight(),
            &mut self.fw_distances,
            &mut self.fw_parents,
        );
        Self::unpack_path(
            to,
            self.meeting_node,
            false,
            self.customized.cch,
            self.customized.backward_graph().weight(),
            self.customized.forward_graph().weight(),
            &mut self.bw_distances,
            &mut self.bw_parents,
        );

        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != from {
            path.push(self.fw_parents[*path.last().unwrap() as usize]);
        }

        path.reverse();

        while *path.last().unwrap() != to {
            path.push(self.bw_parents[*path.last().unwrap() as usize]);
        }

        for node in &mut path {
            *node = self.customized.cch.node_order().node(*node);
        }

        path
    }

    /// Unpack path from a start node (the meeting node of the CCH query), so that parent pointers point along the unpacked path.
    fn unpack_path(
        origin: NodeId,
        target: NodeId,
        forward: bool,
        cch: &CCH,
        weights: &[Weight],
        other_weights: &[Weight],
        distances: &mut TimestampedVector<Weight>,
        parents: &mut [NodeId],
    ) {
        let mut current = target;
        while current != origin {
            let pred = parents[current as usize];
            let weight = distances[current as usize] - distances[pred as usize];

            let unpacked = if forward {
                cch.unpack_arc(pred, current, weight, weights, other_weights)
            } else {
                cch.unpack_arc(current, pred, weight, other_weights, weights)
            };
            if let Some((middle, first_weight, second_weight)) = unpacked {
                parents[current as usize] = middle;
                parents[middle as usize] = pred;
                distances[middle as usize] = distances[pred as usize] + if forward { first_weight } else { second_weight };
            } else {
                current = pred;
            }
        }
    }
}

pub struct PathServerWrapper<'s, 'a, CCH>(&'s mut Server<'a, CCH>, Query);

impl<'s, 'a, CCH: CCHT> PathServer for PathServerWrapper<'s, 'a, CCH> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<'a, CCH: CCHT> QueryServer for Server<'a, CCH> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, 'a, CCH>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
