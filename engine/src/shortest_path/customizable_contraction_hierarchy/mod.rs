use super::*;
use crate::shortest_path::node_order::NodeOrder;
use crate::benchmark::report_time;
use crate::report::*;

use std::ops::{Index, IndexMut};
use std::cmp::Ordering;

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

    fn merge_neighbors(&mut self, others: &[NodeId]) {
        let mut new_edges = Vec::with_capacity(self.edges.len() + others.len());

        let mut self_iter = self.edges.iter().peekable();
        let mut other_iter = others.iter().peekable();

        loop {
            match (self_iter.peek(), other_iter.peek()) {
                (Some(&&self_neighbor), Some(&&other_neighbor)) => {
                    match self_neighbor.cmp(&other_neighbor) {
                        Ordering::Less => {
                            new_edges.push(self_neighbor);
                            self_iter.next();
                        },
                        Ordering::Greater => {
                            new_edges.push(other_neighbor);
                            other_iter.next();
                        },
                        Ordering::Equal => {
                            new_edges.push(self_neighbor);
                            self_iter.next();
                            other_iter.next();
                        },
                    }
                },
                (Some(&&neighbor), None) => {
                    new_edges.push(neighbor);
                    self_iter.next();
                },
                (None, Some(&&neighbor)) => {
                    new_edges.push(neighbor);
                    other_iter.next();
                },
                _ => break,
            }
        }

        self.edges = new_edges;
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

        // translate node ids to ranks
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

        // make graph undirected
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

        for (node_id, node) in nodes.iter_mut().enumerate() {
            node.edges.retain(|&neighbor| neighbor > node_id as NodeId); // remove down arcs
            node.edges.sort();
        }

        ContractionGraph {
            nodes,
            node_order,
            original_graph: graph
        }
    }

    fn contract(mut self) -> ContractedGraph<'a, Graph> {
        report!("algo", "CCH Contraction");
        report_time("CCH Contraction", || {
            let mut num_shortcut_arcs = 0;
            let mut graph = self.partial_graph();

            while let Some((node, mut subgraph)) = graph.remove_lowest() {
                if let Some((&lowest_neighbor, other_neighbors)) = node.edges.split_first() {
                    let prev_deg = subgraph[lowest_neighbor as usize].edges.len();
                    subgraph[lowest_neighbor as usize].merge_neighbors(other_neighbors);
                    num_shortcut_arcs += subgraph[lowest_neighbor as usize].edges.len() - prev_deg;
                }

                graph = subgraph;
            }

            report!("num_arcs_inserted", num_shortcut_arcs);
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
            let subgraph = PartialContractionGraph { nodes: other_nodes, id_offset: self.id_offset + 1 };
            Some((node, subgraph))
        } else {
            None
        }
    }
}

impl<'a> Index<usize> for PartialContractionGraph<'a> {
    type Output = Node;

    fn index(&self, idx: usize) -> &Node {
        &self.nodes[idx - self.id_offset as usize]
    }
}

impl<'a> IndexMut<usize> for PartialContractionGraph<'a> {
    fn index_mut(&mut self, idx: usize) -> &mut Node {
        &mut self.nodes[idx - self.id_offset as usize]
    }
}

#[derive(Debug)]
pub struct ContractedGraph<'a, Graph: for<'b> LinkIterGraph<'b> + 'a>(ContractionGraph<'a, Graph>);

pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, node_order: NodeOrder) -> CCHGraph {
    CCHGraph::new(ContractionGraph::new(graph, node_order).contract())
}
