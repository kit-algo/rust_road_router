//! Contraction Hierarchy query server.
//!
//! Actually not much more than a bidirectional dijkstra with a different stopping criterion.
//! And more complicated path unpacking.
//! This works because the augmented graph was split into an upward and an downward part.
//! This implicitly makes sure, that both searches only go to higher ranked nodes.

use super::*;

pub struct Server {
    forward: OwnedGraph,
    backward: OwnedGraph,
    forward_data: DijkstraData<Weight>,
    backward_data: DijkstraData<Weight>,
    meeting_node: NodeId,
    shortcut_middle_nodes: Option<(Vec<NodeId>, Vec<NodeId>)>,
    order: NodeOrder,
}

impl Server {
    pub fn new(ch: ContractionHierarchy, order: NodeOrder) -> Server {
        let n = ch.forward.num_nodes();
        Server {
            forward: ch.forward,
            backward: ch.backward,
            forward_data: DijkstraData::new(n),
            backward_data: DijkstraData::new(n),
            meeting_node: 0,
            shortcut_middle_nodes: ch.middle_nodes,
            order,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.order.rank(from);
        let to = self.order.rank(to);

        // initialize
        let mut tentative_distance = INFINITY;

        let mut fw_ops = DefaultOps();
        let mut bw_ops = DefaultOps();
        let mut forward_dijkstra = DijkstraRun::query(&self.forward, &mut self.forward_data, &mut fw_ops, Query { from, to });
        let mut backward_dijkstra = DijkstraRun::query(&self.backward, &mut self.backward_data, &mut bw_ops, Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;
        let mut forward_done = false;
        let mut backward_done = false;

        // compare tentative distance to both directions progress individually rather than the sum!
        while (tentative_distance > forward_progress || tentative_distance > backward_progress) && !(forward_done && backward_done) {
            if backward_done || (forward_progress <= backward_progress && !forward_done) {
                if let Some(node) = forward_dijkstra.next() {
                    let distance = *forward_dijkstra.tentative_distance(node);
                    forward_progress = distance;

                    if distance + backward_dijkstra.tentative_distance(node) < tentative_distance {
                        tentative_distance = distance + backward_dijkstra.tentative_distance(node);
                        self.meeting_node = node;
                    }

                    if node == to {
                        forward_done = true;
                    }
                } else {
                    forward_done = true;
                }
            } else {
                if let Some(node) = backward_dijkstra.next() {
                    let distance = *backward_dijkstra.tentative_distance(node);
                    backward_progress = distance;

                    if distance + forward_dijkstra.tentative_distance(node) < tentative_distance {
                        tentative_distance = distance + forward_dijkstra.tentative_distance(node);
                        self.meeting_node = node;
                    }

                    if node == from {
                        backward_done = true;
                    }
                } else {
                    backward_done = true;
                }
            }
        }

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    // Ugly path unpacking.
    // Should probably switch to something similar as in CCH query, which utilizes the parent pointers in the dijkstra struct.
    fn path(&self, query: Query) -> Vec<NodeId> {
        use std::collections::LinkedList;

        let &(ref forward_middle_nodes, ref backward_middle_nodes) = self.shortcut_middle_nodes.as_ref().unwrap();
        let mut forwad_path = LinkedList::new();
        forwad_path.push_front(self.meeting_node);

        while *forwad_path.front().unwrap() != query.from {
            let next = self.forward_data.predecessors[*forwad_path.front().unwrap() as usize].0;
            let mut shortcut_stack = vec![next];

            while let Some(node) = shortcut_stack.pop() {
                let next = self.forward_data.predecessors[*forwad_path.front().unwrap() as usize].0;
                let middle = forward_middle_nodes[self.forward.edge_indices(node, next).next().unwrap().0 as usize];
                if middle < self.forward.num_nodes() as NodeId {
                    shortcut_stack.push(node);
                    shortcut_stack.push(middle);
                } else {
                    forwad_path.push_front(node);
                }
            }
        }

        let mut backward_path = LinkedList::new();
        backward_path.push_back(self.meeting_node);

        while *backward_path.back().unwrap() != query.to {
            let next = self.backward_data.predecessors[*backward_path.back().unwrap() as usize].0;
            let mut shortcut_stack = vec![next];

            while let Some(node) = shortcut_stack.pop() {
                let next = self.backward_data.predecessors[*backward_path.back().unwrap() as usize].0;
                let middle = backward_middle_nodes[self.backward.edge_indices(node, next).next().unwrap().0 as usize];
                if middle < self.backward.num_nodes() as NodeId {
                    shortcut_stack.push(node);
                    shortcut_stack.push(middle);
                } else {
                    backward_path.push_back(node);
                }
            }
        }

        forwad_path.pop_back();
        forwad_path.append(&mut backward_path);

        for node in &mut forwad_path {
            *node = self.order.node(*node);
        }

        forwad_path.into_iter().collect()
    }
}

pub struct PathServerWrapper<'s>(&'s Server, Query);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl QueryServer for Server {
    type P<'s> = PathServerWrapper<'s>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
