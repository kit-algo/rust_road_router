use super::*;
use crate::shortest_path::node_order::NodeOrder;
use crate::in_range_option::InRangeOption;
use crate::benchmark::measure;

pub mod cch_graph;

use self::cch_graph::CCHGraph;

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    Existed
}

#[derive(Debug)]
struct Node {
    edges: Vec<NodeId>,
}

impl Node {
    fn insert(&mut self, node: NodeId) -> ShortcutResult {
        for &other in &self.edges {
            if node == other {
                return ShortcutResult::Existed
            }
        }

        self.edges.push(node);
        ShortcutResult::NewShortcut
    }

    fn remove(&mut self, to: NodeId) {
        let pos = self.edges.iter().position(|&node| to == node).unwrap();
        self.edges.swap_remove(pos);
        debug_assert_eq!(self.edges.iter().position(|&node| to == node), None);
    }
}

#[derive(Debug)]
struct ContractionGraph<'a, Graph: for<'b> LinkIterGraph<'b> + 'a> {
    nodes: Vec<Node>,
    node_order: NodeOrder,
    original_graph: &'a Graph
}

impl<'a, Graph: for<'b> LinkIterGraph<'b>> ContractionGraph<'a, Graph> {
    fn new(graph: &'a Graph, node_order: NodeOrder) -> ContractionGraph<'a, Graph> {
        let n = graph.num_nodes() as NodeId;

        let mut nodes: Vec<Node> = (0..n).map(|node| {
            let old_node_id = node_order.node(node);
            let edges = graph.neighbor_iter(old_node_id)
                .map(|Link { node: neighbor, .. }| {
                    debug_assert_ne!(old_node_id, neighbor);
                    node_order.rank(neighbor)
                })
                .collect();
            Node { edges }
        }).collect();

        for node_id in 0..n {
            let (nodes_before, nodes_after_including_node) = nodes.split_at_mut(node_id as usize);
            let (node, nodes_after) = nodes_after_including_node.split_first_mut().unwrap();

            debug_assert_eq!(nodes_before.len(), node_id as usize);
            debug_assert_eq!(nodes_after.len(), (n - node_id - 1) as usize);

            for &neighbor in &node.edges {
                debug_assert_ne!(node_id, neighbor);
                if neighbor < node_id {
                    nodes_before[neighbor as usize].insert(node_id);
                } else {
                    nodes_after[(neighbor - 1 - node_id) as usize].insert(node_id);
                }
            }
        }

        ContractionGraph {
            nodes,
            node_order,
            original_graph: graph
        }
    }

    fn contract(mut self) -> ContractedGraph<'a, Graph> {
        measure("CCH Contraction", || {
            let mut num_shortcut_arcs = 0;
            let mut graph = self.partial_graph();

            while let Some((node, mut subgraph)) = graph.remove_lowest() {
                for &from in &node.edges {
                    for &to in &node.edges {
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
        });


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
        for &from in &node.edges {
            debug_assert!(from > node_id);
            self.nodes[(from - self.id_offset) as usize].remove(node_id);
        }
    }

    fn insert(&mut self, from: NodeId, to: NodeId) -> ShortcutResult {
        debug_assert_ne!(from, to);
        let out_result = self.nodes[(from - self.id_offset) as usize].insert(to);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert(from);

        debug_assert_eq!(out_result, in_result);
        out_result
    }
}

#[derive(Debug)]
pub struct ContractedGraph<'a, Graph: for<'b> LinkIterGraph<'b> + 'a>(ContractionGraph<'a, Graph>);

impl<'a, Graph: for<'b> LinkIterGraph<'b>> ContractedGraph<'a, Graph> {
    fn elimination_tree(&self) -> Vec<InRangeOption<NodeId>> {
        let n = self.0.original_graph.num_nodes();
        let mut elimination_tree = vec![InRangeOption::new(None); n];

        for (rank, node) in self.0.nodes.iter().enumerate() {
            elimination_tree[rank] = InRangeOption::new(node.edges.iter().cloned().min());
            debug_assert!(elimination_tree[rank].value().unwrap_or(n as NodeId) as usize > rank);
        }

        elimination_tree
    }
}

pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, node_order: NodeOrder) -> CCHGraph {
    CCHGraph::new(ContractionGraph::new(graph, node_order).contract())
}
