use super::*;
use shortest_path::DijkstrableGraph;

#[derive(Debug)]
#[allow(dead_code)]
struct CHGraph {
    upward: FirstOutGraph,
    downward: FirstOutGraph,
    node_order: Vec<u32>
}

impl CHGraph {
    #[allow(dead_code)]
    pub fn customize(&mut self) {
        let n = self.upward.num_nodes();
        let mut outgoing_weights = vec![INFINITY; n];
        let mut incoming_weights = vec![INFINITY; n];

        for current_node in 0..n {
            for Link { node, weight } in self.downward.neighbor_iter(current_node as NodeId) {
                incoming_weights[node as usize] = weight;
            }
            for Link { node, weight } in self.upward.neighbor_iter(current_node as NodeId) {
                outgoing_weights[node as usize] = weight;
            }

            for Link { node, weight } in self.downward.neighbor_iter(current_node as NodeId) {
                for (&mut target, shortcut_weight) in self.upward.neighbor_iter_mut(node) {
                    if weight + outgoing_weights[target as usize] < *shortcut_weight {
                        *shortcut_weight = weight + outgoing_weights[target as usize];
                    }
                }
            }
            for Link { node, weight } in self.upward.neighbor_iter(current_node as NodeId) {
                for (&mut target, shortcut_weight) in self.downward.neighbor_iter_mut(node) {
                    if weight + incoming_weights[target as usize] < *shortcut_weight {
                        *shortcut_weight = weight + incoming_weights[target as usize];
                    }
                }
            }

            for Link { node, .. } in self.downward.neighbor_iter(current_node as NodeId) {
                incoming_weights[node as usize] = INFINITY;
            }
            for Link { node, .. } in self.upward.neighbor_iter(current_node as NodeId) {
                outgoing_weights[node as usize] = INFINITY;
            }
        }
    }
}
