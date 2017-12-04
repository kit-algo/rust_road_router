use super::*;
use super::first_out_graph::FirstOutGraph;
use shortest_path::node_order::NodeOrder;
use ::inrange_option::InrangeOption;

mod cch_graph;

use self::cch_graph::CCHGraph;

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    Existed
}

#[derive(Debug)]
struct Node {
    outgoing: Vec<(NodeId, InrangeOption<EdgeId>)>,
    incoming: Vec<(NodeId, InrangeOption<EdgeId>)>
}

impl Node {
    fn insert_outgoing(&mut self, to: NodeId) -> ShortcutResult {
        Node::insert(&mut self.outgoing, to)
    }

    fn insert_incoming(&mut self, from: NodeId) -> ShortcutResult {
        Node::insert(&mut self.incoming, from)
    }

    fn insert(links: &mut Vec<(NodeId, InrangeOption<EdgeId>)>, node: NodeId) -> ShortcutResult {
        for &(other, _) in links.iter() {
            if node == other {
                return ShortcutResult::Existed
            }
        }

        links.push((node, InrangeOption::new(None)));
        ShortcutResult::NewShortcut
    }

    fn remove_outgoing(&mut self, to: NodeId) {
        let pos = self.outgoing.iter().position(|&(node, _)| to == node).unwrap();
        self.outgoing.swap_remove(pos);
        debug_assert_eq!(self.outgoing.iter().position(|&(node, _)| to == node), None);
    }

    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&(node, _)| from == node).unwrap();
        self.incoming.swap_remove(pos);
        debug_assert_eq!(self.incoming.iter().position(|&(node, _)| from == node), None);
    }
}

#[derive(Debug)]
struct ContractionGraph<'a> {
    nodes: Vec<Node>,
    node_order: NodeOrder,
    original_graph: &'a FirstOutGraph
}

impl<'a> ContractionGraph<'a> {
    fn new(graph: &FirstOutGraph, node_order: NodeOrder) -> ContractionGraph {
        let n = graph.num_nodes() as NodeId;

        let nodes = {
            let outs: Vec<Vec<(NodeId, InrangeOption<EdgeId>)>> = (0..n).map(|node| {
                let old_node_id = node_order.node(node);
                graph.neighbor_iter(old_node_id)
                    .map(|Link { node: neighbor, .. }| { debug_assert_ne!(old_node_id, neighbor); node_order.rank(neighbor) })
                    .zip(graph.neighbor_edge_indices(old_node_id).map(|edge_id| InrangeOption::new(Some(edge_id))))
                    .collect()
            }).collect();

            let mut ins = vec![vec![]; n as usize];
            for node in 0..n {
                for &(neighbor, edge_id) in outs[node as usize].iter() {
                    ins[neighbor as usize].push((node, edge_id));
                }
            }

            outs.into_iter().zip(ins.into_iter()).map(|(outgoing, incoming)| Node { outgoing, incoming } ).collect()
        };

        ContractionGraph {
            nodes,
            node_order,
            original_graph: graph
        }
    }

    fn contract(mut self) -> ContractedGraph<'a> {
        {
            let mut num_shortcut_arcs = 0;
            let mut graph = self.partial_graph();

            while let Some((node, mut subgraph)) = graph.remove_lowest() {
                for &(from, _) in node.incoming.iter() {
                    for &(to, _) in node.outgoing.iter() {
                        if from != to {
                            match subgraph.insert(from, to) {
                                ShortcutResult::NewShortcut => num_shortcut_arcs += 1,
                                ShortcutResult::Existed => (),
                            }
                        }
                    }
                }

                graph = subgraph;
            }

            println!("Number of arcs inserted during contraction: {:?}", num_shortcut_arcs);
        }


        ContractedGraph(self)
    }

    fn partial_graph(&mut self) -> PartialContractionGraph {
        PartialContractionGraph {
            nodes: &mut self.nodes[..],
            id_offset: 0
        }
    }
}

#[derive(Debug)]
struct PartialContractionGraph<'a> {
    nodes: &'a mut [Node],
    id_offset: NodeId
}

impl<'a> PartialContractionGraph<'a> {
    fn remove_lowest(self) -> Option<(&'a Node, PartialContractionGraph<'a>)> {
        if let Some((node, other_nodes)) = self.nodes.split_first_mut() {
            let mut subgraph = PartialContractionGraph { nodes: other_nodes, id_offset: self.id_offset + 1 };
            subgraph.remove_edges_to_removed(&node);
            Some((node, subgraph))
        } else {
            None
        }
    }

    fn remove_edges_to_removed(&mut self, node: &Node) {
        let node_id = self.id_offset - 1;
        for &(from, _) in node.incoming.iter() {
            debug_assert!(from > node_id);
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(node_id);
        }
        for &(to, _) in node.outgoing.iter() {
            debug_assert!(to > node_id);
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(node_id);
        }
    }

    fn insert(&mut self, from: NodeId, to: NodeId) -> ShortcutResult {
        debug_assert_ne!(from, to);
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_outgoing(to);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_incoming(from);

        debug_assert_eq!(out_result, in_result);
        out_result
    }
}

#[derive(Debug)]
pub struct ContractedGraph<'a>(ContractionGraph<'a>);

impl<'a> ContractedGraph<'a> {
    fn elimination_trees(&self) -> Vec<InrangeOption<NodeId>> {
        let n = self.0.original_graph.num_nodes();
        let mut elimination_tree = vec![InrangeOption::new(None); n];

        for (rank, node) in self.0.nodes.iter().enumerate() {
            elimination_tree[rank] = InrangeOption::new(node.outgoing.iter().chain(node.incoming.iter()).map(|&(node, _)| node).min());
            debug_assert!(elimination_tree[rank].value().unwrap_or(n as NodeId) as usize > rank);
        }

        elimination_tree
    }
}

pub fn contract(graph: &FirstOutGraph, node_order: NodeOrder) -> CCHGraph {
    CCHGraph::new(ContractionGraph::new(graph, node_order).contract())
}
