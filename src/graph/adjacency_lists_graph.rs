use super::*;

#[derive(Debug, PartialEq)]
pub enum ShortcutResult {
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
}

#[derive(Debug)]
pub struct AdjacencyListsGraph {
    nodes: Vec<Node>
}

impl AdjacencyListsGraph {
    pub fn new(n: NodeId) -> AdjacencyListsGraph {
        AdjacencyListsGraph { nodes: (0..n).map(|_| Node::new() ).collect() }
    }

    // TODO use less memory
    pub fn from_first_out_graph(graph: first_out_graph::FirstOutGraph) -> AdjacencyListsGraph {
        let n: NodeId = graph.num_nodes() as NodeId;

        let outs = (0..n).map(|node| graph.neighbor_iter(node).collect() );
        let reversed = graph.reverse();
        let ins = (0..n).map(|node| reversed.neighbor_iter(node).collect() );

        AdjacencyListsGraph {
            nodes: outs.zip(ins).map(|(outgoing, incoming)| Node { outgoing, incoming } ).collect()
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn insert_or_decrease(&mut self, from: NodeId, to: NodeId, weight: Weight) -> ShortcutResult {
        let out_result = self.nodes[from as usize].insert_or_decrease_outgoing(to, weight);
        let in_result = self.nodes[to as usize].insert_or_decrease_incoming(from, weight);

        assert!(out_result == in_result);
        out_result
    }

    pub fn outgoing_iter(&self, node: NodeId) -> std::slice::Iter<Link> {
        self.nodes[node as usize].outgoing.iter()
    }

    pub fn incmoing_iter(&self, node: NodeId) -> std::slice::Iter<Link> {
        self.nodes[node as usize].incoming.iter()
    }
}
