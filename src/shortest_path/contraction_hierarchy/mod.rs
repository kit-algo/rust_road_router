use super::*;
use self::first_out_graph::{FirstOutGraph, DijkstrableGraph};
use std;

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    ShortenedExisting,
    ShorterExisting
}

#[derive(Debug)]
struct Node {
    outgoing: Vec<Link>,
    incoming: Vec<Link>
}

impl Node {
    fn new() -> Node {
        Node {
            outgoing: Vec::new(),
            incoming: Vec::new()
        }
    }

    fn insert_or_decrease_outgoing(&mut self, to: NodeId, weight: Weight) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.outgoing, to, weight)
    }

    fn insert_or_decrease_incoming(&mut self, from: NodeId, weight: Weight) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.incoming, from, weight)
    }

    fn insert_or_decrease(links: &mut Vec<Link>, node: NodeId, weight: Weight) -> ShortcutResult {
        for &mut Link { node: other, weight: ref mut other_weight } in links.iter_mut() {
            if node == other {
                if weight < *other_weight {
                    *other_weight = weight;
                    return ShortcutResult::ShortenedExisting;
                } else {
                    return ShortcutResult::ShorterExisting;
                }
            }
        }

        links.push(Link { node, weight });
        ShortcutResult::NewShortcut
    }

    fn remove_outgoing(&mut self, to: NodeId) {
        let pos = self.outgoing.iter().position(|&Link { node, .. }| to == node).unwrap();
        self.outgoing.swap_remove(pos);
    }

    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&Link { node, .. }| from == node).unwrap();
        self.incoming.swap_remove(pos);
    }
}

#[derive(Debug)]
struct ContractionGraph {
    nodes: Vec<Node>,
    node_order: Vec<NodeId>
}

impl ContractionGraph {
    fn new(graph: FirstOutGraph, node_order: Vec<NodeId>) -> ContractionGraph {
        let n = graph.num_nodes();
        let mut node_ranks = vec![0; n];
        for (i, &node) in node_order.iter().enumerate() {
            node_ranks[node as usize] = i as u32;
        }

        let nodes = {
            let outs = (0..n).map(|node| graph.neighbor_iter(node_order[node]).map(|Link { node, weight }| Link { node: node_ranks[node as usize], weight }).collect() );
            let reversed = graph.reverse();
            let ins = (0..n).map(|node| reversed.neighbor_iter(node_order[node]).map(|Link { node, weight }| Link { node: node_ranks[node as usize], weight }).collect() );
            outs.zip(ins).map(|(outgoing, incoming)| Node { outgoing, incoming } ).collect()
        };

        ContractionGraph {
            nodes,
            node_order
        }
    }

    fn num_nodes(&self) -> usize {
        self.nodes.len()
    }


    fn outgoing_iter(&self, node: NodeId) -> std::slice::Iter<Link> {
        self.nodes[node as usize].outgoing.iter()
    }

    fn incmoing_iter(&self, node: NodeId) -> std::slice::Iter<Link> {
        self.nodes[node as usize].incoming.iter()
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
    fn remove_lowest(&'a mut self) -> Option<(&'a Node, PartialContractionGraph<'a>)> {
        if let Some((node, other_nodes)) = self.nodes.split_first_mut() {
            let mut subgraph = PartialContractionGraph { nodes: other_nodes, id_offset: self.id_offset + 1 };
            subgraph.remove_edges_to_removed(&node);
            Some((node, subgraph))
        } else {
            None
        }
    }

    fn remove_edges_to_removed(&mut self, node: &Node) {
        for &Link { node: from, .. } in node.incoming.iter() {
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(self.id_offset - 1);
        }
        for &Link { node: to, .. } in node.outgoing.iter() {
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(self.id_offset - 1);
        }
    }

    fn insert_or_decrease(&mut self, from: NodeId, to: NodeId, weight: Weight) -> ShortcutResult {
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_or_decrease_outgoing(to, weight);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_or_decrease_incoming(from, weight);

        assert!(out_result == in_result);
        out_result
    }

    fn contract(&'a mut self) {
        if let Some((node, mut subgraph)) = self.remove_lowest() {
            for &Link { node: from, weight: from_weight } in node.incoming.iter() {
                for &Link { node: to, weight: to_weight } in node.outgoing.iter() {
                    subgraph.insert_or_decrease(from, to, from_weight + to_weight);
                }
            }

            subgraph.contract();
        }
    }
}

pub fn contract(graph: FirstOutGraph, node_order: Vec<NodeId>) -> () {
    let mut graph = ContractionGraph::new(graph, node_order);
    graph.partial_graph().contract();
    // graph.as_first_out_graph()
}

