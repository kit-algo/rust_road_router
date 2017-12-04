use super::*;
use shortest_path::node_order::NodeOrder;
use shortest_path::DijkstrableGraph;
use ::inrange_option::InrangeOption;
use ::benchmark::measure;

#[derive(Debug)]
#[allow(dead_code)]
pub struct CCHGraph {
    upward: FirstOutGraph,
    downward: FirstOutGraph,
    node_order: NodeOrder,
    original_edge_to_ch_edge: Vec<InrangeOption<EdgeId>>,
    elimination_tree: Vec<InrangeOption<NodeId>>
}

impl CCHGraph {
    pub(super) fn new(contracted_graph: ContractedGraph) -> CCHGraph {
        let elimination_tree = contracted_graph.elimination_trees();
        let ContractedGraph(contracted_graph) = contracted_graph;
        let node_order = contracted_graph.node_order;

        let (outgoing, incoming) = contracted_graph.nodes.into_iter().map(|node| {
            (node.outgoing, node.incoming)
        }).unzip();

        let mut original_edge_to_ch_edge = vec![InrangeOption::<EdgeId>::new(None); contracted_graph.original_graph.num_arcs()];

        CCHGraph {
            upward: Self::adjancecy_lists_to_first_out_graph(outgoing, &mut original_edge_to_ch_edge),
            downward: Self::adjancecy_lists_to_first_out_graph(incoming, &mut original_edge_to_ch_edge),
            node_order,
            original_edge_to_ch_edge,
            elimination_tree
        }
    }

    fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Vec<(NodeId, InrangeOption<EdgeId>)>>, original_edge_to_ch_edge: &mut Vec<InrangeOption<EdgeId>>) -> FirstOutGraph {
        let n = adjancecy_lists.len();

        let first_out: Vec<EdgeId> = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.len() as EdgeId);
            FirstOutGraph::degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(first_out.len(), n + 1);

        let (head, original_edge_ids): (Vec<NodeId>, Vec<InrangeOption<EdgeId>>) = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter())
            .unzip();

        for (ch_edge_index, original_edge_id) in original_edge_ids.into_iter().enumerate() {
            if let Some(original_edge_id) = original_edge_id.value() {
                debug_assert_eq!(original_edge_to_ch_edge[original_edge_id as usize].value(), None);
                original_edge_to_ch_edge[original_edge_id as usize] = InrangeOption::new(Some(ch_edge_index as EdgeId));
            }
        }

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
                    if let Some(ch_edge_id) = self.original_edge_to_ch_edge[edge_id as usize].value() {
                        if self.node_order.rank(node) < self.node_order.rank(neighbor) {
                            upward_weights[ch_edge_id as usize] = weight;
                        } else {
                            downward_weights[ch_edge_id as usize] = weight;
                        }
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
