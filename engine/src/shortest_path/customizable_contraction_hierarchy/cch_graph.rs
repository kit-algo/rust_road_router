use super::*;
use shortest_path::node_order::NodeOrder;
use shortest_path::DijkstrableGraph;
use ::inrange_option::InrangeOption;
use ::benchmark::measure;

#[derive(Debug)]
#[allow(dead_code)]
pub struct CCHGraph {
    // TODO get rid of pub
    pub upward: FirstOutGraph,
    pub downward: FirstOutGraph,
    pub node_order: NodeOrder,
    original_edge_to_ch_edge: Vec<EdgeId>,
    pub elimination_tree: Vec<InrangeOption<NodeId>>,
}

impl CCHGraph {
    pub(super) fn new(contracted_graph: ContractedGraph) -> CCHGraph {
        let elimination_tree = contracted_graph.elimination_tree();
        let ContractedGraph(contracted_graph) = contracted_graph;
        let node_order = contracted_graph.node_order;
        let original_graph = contracted_graph.original_graph;

        let graph = Self::adjancecy_lists_to_first_out_graph(contracted_graph.nodes);
        let n = graph.num_nodes() as NodeId;
        let original_edge_to_ch_edge = (0..n).flat_map(|node| {
            {
                let graph = &graph;
                let node_order = &node_order;

                original_graph.neighbor_iter(node).map(move |Link { node: neighbor, .. }| {
                    let node_rank = node_order.rank(node);
                    let neighbor_rank = node_order.rank(neighbor);
                    if node_rank < neighbor_rank {
                        graph.edge_index(node_rank, neighbor_rank).unwrap()
                    } else {
                        graph.edge_index(neighbor_rank, node_rank).unwrap()
                    }
                })
            }
        }).collect();
        // let mut original_edge_to_ch_edge = vec![InrangeOption::<EdgeId>::new(None); contracted_graph.original_graph.num_arcs()];

        CCHGraph {
            upward: graph.clone(),
            downward: graph,
            node_order,
            original_edge_to_ch_edge,
            elimination_tree,
        }
    }

    fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Node>) -> FirstOutGraph {
        let n = adjancecy_lists.len();

        let first_out: Vec<EdgeId> = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.edges.len() as EdgeId);
            FirstOutGraph::degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(first_out.len(), n + 1);

        let head: Vec<NodeId> = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.edges.into_iter())
            .collect();

        let m = head.len();
        FirstOutGraph::new(first_out, head, vec![INFINITY; m])
    }

    pub fn customize(&mut self, metric: &FirstOutGraph) {
        let n = self.upward.num_nodes() as NodeId;

        measure("CCH apply weights", || {
            let mut upward_weights = vec![INFINITY; self.upward.num_arcs()];
            let mut downward_weights = vec![INFINITY; self.downward.num_arcs()];

            for node in 0..n {
                for (edge_id, Link { node: neighbor, weight }) in metric.neighbor_edge_indices(node).zip(metric.neighbor_iter(node)) {
                    let ch_edge_id = self.original_edge_to_ch_edge[edge_id as usize];

                    if self.node_order.rank(node) < self.node_order.rank(neighbor) {
                        upward_weights[ch_edge_id as usize] = weight;
                    } else {
                        downward_weights[ch_edge_id as usize] = weight;
                    }
                }
            }

            self.upward.swap_weights(&mut upward_weights);
            self.downward.swap_weights(&mut downward_weights);
        });

        measure("CCH Customization", || {
            let mut node_outgoing_weights = vec![INFINITY; n as usize];
            let mut node_incoming_weights = vec![INFINITY; n as usize];

            for current_node in 0..n {
                for Link { node, weight } in self.downward.neighbor_iter(current_node) {
                    node_incoming_weights[node as usize] = weight;
                }
                for Link { node, weight } in self.upward.neighbor_iter(current_node) {
                    node_outgoing_weights[node as usize] = weight;
                }

                for Link { node, weight } in self.downward.neighbor_iter(current_node) {
                    for (&mut target, shortcut_weight) in self.upward.neighbor_iter_mut(node) {
                        if weight + node_outgoing_weights[target as usize] < *shortcut_weight {
                            *shortcut_weight = weight + node_outgoing_weights[target as usize];
                        }
                    }
                }
                for Link { node, weight } in self.upward.neighbor_iter(current_node) {
                    for (&mut target, shortcut_weight) in self.downward.neighbor_iter_mut(node) {
                        if weight + node_incoming_weights[target as usize] < *shortcut_weight {
                            *shortcut_weight = weight + node_incoming_weights[target as usize];
                        }
                    }
                }

                for Link { node, .. } in self.downward.neighbor_iter(current_node) {
                    node_incoming_weights[node as usize] = INFINITY;
                }
                for Link { node, .. } in self.upward.neighbor_iter(current_node) {
                    node_outgoing_weights[node as usize] = INFINITY;
                }
            }
        });

    }
}
