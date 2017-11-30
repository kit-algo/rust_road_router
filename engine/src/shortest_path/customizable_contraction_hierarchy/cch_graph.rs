use super::*;
use shortest_path::node_order::NodeOrder;
use shortest_path::DijkstrableGraph;
use ::inrange_option::InrangeOption;

#[derive(Debug)]
#[allow(dead_code)]
pub struct CCHGraph {
    upward: FirstOutGraph,
    downward: FirstOutGraph,
    node_order: NodeOrder,
    // weight_mapping: Vec<u64>,
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

        let mut original_edge_to_ch_edge = vec![InrangeOption::<EdgeId>::new(None); contracted_graph.original_graph.num_arcs()]; // TODO original m

        CCHGraph {
            upward: Self::adjancecy_lists_to_first_out_graph(outgoing, &mut original_edge_to_ch_edge),
            downward: Self::adjancecy_lists_to_first_out_graph(incoming, &mut original_edge_to_ch_edge),
            node_order,
            elimination_tree
        }
    }

    fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Vec<(NodeId, InrangeOption<EdgeId>)>>, original_edge_to_ch_edge: &mut Vec<InrangeOption<EdgeId>>) -> FirstOutGraph {
        let n = adjancecy_lists.len();
        // create first_out array by doing a prefix sum over the adjancecy list sizes
        let first_out: Vec<EdgeId> = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.len() as EdgeId);
            FirstOutGraph::degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(first_out.len(), n + 1);

        // append all adjancecy list and split the pairs into two seperate vectors
        let (head, original_edge_ids): (Vec<NodeId>, Vec<InrangeOption<EdgeId>>) = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter())
            .unzip();


        for (ch_edge_index, original_edge_id) in original_edge_ids.into_iter().enumerate() {
            match original_edge_id.value() {
                Some(original_edge_id) => {
                    debug_assert_eq!(original_edge_to_ch_edge[original_edge_id as usize].value(), None);
                    original_edge_to_ch_edge[original_edge_id as usize] = InrangeOption::new(Some(ch_edge_index as EdgeId));
                },
                None => (),
            }
        }

        let m = head.len();
        FirstOutGraph::new(first_out, head, vec![INFINITY; m])
    }

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
