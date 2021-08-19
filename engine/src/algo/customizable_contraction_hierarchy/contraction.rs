//! Algorithms for first phase of CCH preprocessing.
//!
//! This is an implementation of the FAST (FAst and Simple Triangulation algorithm).
//! It is heavily based on a linear time chordality check.
//! This implementation is not guaranteed to run in linear time, but is practically faster than the linear version.

use super::*;
use crate::{datastr::graph::first_out_graph::degrees_to_first_out, report::benchmark::report_time, report::*};
use std::{
    cmp::Ordering,
    ops::{Index, IndexMut},
};

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    Existed,
}

// During contraction, nodes are represented as Vecs of neighbor nodes.
#[derive(Debug)]
struct Node {
    edges: Vec<NodeId>,
}

impl Node {
    // inserts a node into the neighborhood unless it already exists
    fn insert(&mut self, node: NodeId) -> ShortcutResult {
        for &other in &self.edges {
            if node == other {
                return ShortcutResult::Existed;
            }
        }

        self.edges.push(node);
        ShortcutResult::NewShortcut
    }

    // Merges the neighborhood of another node into this node.
    // Efficient because neighborhoods are stored sorted.
    fn merge_neighbors(&mut self, others: &[NodeId]) {
        let mut new_edges = Vec::with_capacity(self.edges.len() + others.len());

        // Neighborhoods are kept sorted, so we do a coordinated linear sweep and push into `new_edges`
        let mut self_iter = self.edges.iter().peekable();
        let mut other_iter = others.iter().peekable();

        loop {
            match (self_iter.peek(), other_iter.peek()) {
                (Some(&&self_neighbor), Some(&&other_neighbor)) => match self_neighbor.cmp(&other_neighbor) {
                    Ordering::Less => {
                        new_edges.push(self_neighbor);
                        self_iter.next();
                    }
                    Ordering::Greater => {
                        new_edges.push(other_neighbor);
                        other_iter.next();
                    }
                    Ordering::Equal => {
                        new_edges.push(self_neighbor);
                        self_iter.next();
                        other_iter.next();
                    }
                },
                (Some(&&neighbor), None) => {
                    new_edges.push(neighbor);
                    self_iter.next();
                }
                (None, Some(&&neighbor)) => {
                    new_edges.push(neighbor);
                    other_iter.next();
                }
                _ => break,
            }
        }

        self.edges = new_edges;
    }
}

#[derive(Debug)]
pub struct ContractionGraph<'a, Graph: 'a> {
    nodes: Vec<Node>,
    node_order: NodeOrder,
    original_graph: &'a Graph,
}

impl<'a, Graph: LinkIterable<NodeIdT>> ContractionGraph<'a, Graph> {
    /// Preprocessing preparation
    pub fn new(graph: &'a Graph, node_order: NodeOrder) -> ContractionGraph<'a, Graph> {
        let n = graph.num_nodes() as NodeId;

        // translate node ids to ranks
        let mut nodes: Vec<Node> = (0..n)
            .map(|node| {
                let old_node_id = node_order.node(node);
                let edges = graph
                    .link_iter(old_node_id)
                    .filter(|neighbor| old_node_id != neighbor.0)
                    .map(|neighbor| {
                        debug_assert_ne!(old_node_id, neighbor.0);
                        node_order.rank(neighbor.0)
                    })
                    .collect();
                Node { edges }
            })
            .collect();

        // make graph undirected
        for node_id in 0..n {
            let (nodes_before, nodes_after_including_node) = nodes.split_at_mut(node_id as usize);
            let (node, nodes_after) = nodes_after_including_node.split_first_mut().unwrap();

            debug_assert_eq!(nodes_before.len(), node_id as usize);
            debug_assert_eq!(nodes_after.len(), (n - node_id - 1) as usize);

            for &neighbor in &node.edges {
                debug_assert_ne!(node_id, neighbor);
                if neighbor < node_id {
                    // insert checks for duplicates, so we have no loops
                    nodes_before[neighbor as usize].insert(node_id);
                } else {
                    // insert checks for duplicates, so we have no loops
                    nodes_after[(neighbor - 1 - node_id) as usize].insert(node_id);
                }
            }
        }

        for (node_id, node) in nodes.iter_mut().enumerate() {
            node.edges.retain(|&neighbor| neighbor > node_id as NodeId); // remove down arcs
            node.edges.sort();
            node.edges.dedup();
        }

        ContractionGraph {
            nodes,
            node_order,
            original_graph: graph,
        }
    }

    /// Main preprocessing work - chordal completion
    pub fn contract(mut self) -> ContractedGraph<'a, Graph> {
        report!("algo", "CCH Contraction");
        report_time_with_key("CCH Contraction", "contraction", || {
            let mut num_shortcut_arcs = 0;
            // We utilize split borrows to make node contraction work well with rusts borrowing rules.
            // The graph representation already contains the node in order of increasing rank.
            // We iteratively split of the lowest ranked node.
            // This is the one that will be contracted next.
            // Contraction does not require mutation of the current node,
            // but we need to modify the neighborhood of nodes of higher rank.

            // we start with the complete graph
            let mut graph = self.partial_graph();

            // split of the lowest node, the one that will be contracted
            while let Some((node, mut subgraph)) = graph.remove_lowest() {
                // find the lowest ranked neighbor - since the neighbors are sorted ascending, this is always the first
                if let Some((&lowest_neighbor, other_neighbors)) = node.edges.split_first() {
                    let prev_deg = subgraph[lowest_neighbor as usize].edges.len();
                    // merge the remaining neighborhood into the neighborhood of the lowest ranked neighbors.
                    // Thats all it takes to perform chordal completion (= CCH contraction) for a given order.
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
            id_offset: 0,
        }
    }
}

// a struct to keep track of the partial graphs during contraction
#[derive(Debug)]
struct PartialContractionGraph<'a> {
    // the nodes in the partial graph
    nodes: &'a mut [Node],
    // slice indices always start at zero, but we need to index by node id,
    // so we remember the number of nodes already contracted
    id_offset: NodeId,
}

impl<'a> PartialContractionGraph<'a> {
    fn remove_lowest(self) -> Option<(&'a Node, PartialContractionGraph<'a>)> {
        if let Some((node, other_nodes)) = self.nodes.split_first_mut() {
            let subgraph = PartialContractionGraph {
                nodes: other_nodes,
                id_offset: self.id_offset + 1,
            };
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

/// Phase one result without any extra info
#[derive(Debug)]
pub struct ContractedGraph<'a, Graph: 'a>(ContractionGraph<'a, Graph>);

impl<'a, Graph: 'a> ContractedGraph<'a, Graph> {
    pub fn decompose(self) -> (UnweightedOwnedGraph, NodeOrder, &'a Graph) {
        (adjancecy_lists_to_first_out_graph(self.0.nodes), self.0.node_order, self.0.original_graph)
    }
}

fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Node>) -> UnweightedOwnedGraph {
    let n = adjancecy_lists.len();

    let first_out: Vec<EdgeId> = {
        let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.edges.len() as EdgeId);
        degrees_to_first_out(degrees).collect()
    };
    debug_assert_eq!(first_out.len(), n + 1);

    let head: Vec<NodeId> = adjancecy_lists.into_iter().flat_map(|neighbors| neighbors.edges.into_iter()).collect();

    UnweightedOwnedGraph::new(first_out, head)
}
