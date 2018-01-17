use super::*;

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    ShortenedExisting,
    ShorterExisting
}

#[derive(Debug)]
struct Node {
    outgoing: Vec<(Link, NodeId)>,
    incoming: Vec<(Link, NodeId)>
}

impl Node {
    fn insert_or_decrease_outgoing(&mut self, to: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.outgoing, to, weight, over)
    }

    fn insert_or_decrease_incoming(&mut self, from: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.incoming, from, weight, over)
    }

    fn insert_or_decrease(links: &mut Vec<(Link, NodeId)>, node: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        for &mut (Link { node: other, weight: ref mut other_weight }, ref mut shortcut_middle) in links.iter_mut() {
            if node == other {
                if weight < *other_weight {
                    *shortcut_middle = over;
                    *other_weight = weight;
                    return ShortcutResult::ShortenedExisting;
                } else {
                    return ShortcutResult::ShorterExisting;
                }
            }
        }

        links.push((Link { node, weight }, over));
        ShortcutResult::NewShortcut
    }

    fn remove_outgoing(&mut self, to: NodeId) {
        let pos = self.outgoing.iter().position(|&(Link { node, .. }, _)| to == node).unwrap();
        self.outgoing.swap_remove(pos);
    }

    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&(Link { node, .. }, _)| from == node).unwrap();
        self.incoming.swap_remove(pos);
    }
}

#[derive(Debug)]
struct ContractionGraph {
    nodes: Vec<Node>,
    node_order: Vec<NodeId>
}

impl ContractionGraph {
    fn new<Graph: for<'a> LinkIterGraph<'a>>(graph: Graph, node_order: Vec<NodeId>) -> ContractionGraph {
        let n = graph.num_nodes();
        let mut node_ranks = vec![0; n];
        for (i, &node) in node_order.iter().enumerate() {
            node_ranks[node as usize] = i as u32;
        }

        let nodes = {
            let outs = (0..n).map(|node|
                graph.neighbor_iter(node_order[node])
                    .map(|Link { node, weight }| (Link { node: node_ranks[node as usize], weight }, n as NodeId))
                    .collect()
            );
            let reversed = graph.reverse();
            let ins = (0..n).map(|node|
                reversed.neighbor_iter(node_order[node])
                    .map(|Link { node, weight }| (Link { node: node_ranks[node as usize], weight }, n as NodeId))
                    .collect()
            );
            outs.zip(ins).map(|(outgoing, incoming)| Node { outgoing, incoming } ).collect()
        };

        ContractionGraph {
            nodes,
            node_order
        }
    }

    fn contract(&mut self) {
        let mut graph = self.partial_graph();

        while let Some((node, mut subgraph)) = graph.remove_lowest() {
            for &(Link { node: from, weight: from_weight }, _) in node.incoming.iter() {
                for &(Link { node: to, weight: to_weight }, _) in node.outgoing.iter() {
                    if subgraph.shortcut_required(from, to, from_weight + to_weight) {
                        let node_id = subgraph.id_offset - 1;
                        subgraph.insert_or_decrease(from, to, from_weight + to_weight, node_id);
                    }
                }
            }

            graph = subgraph;
        }
    }

    fn partial_graph(&mut self) -> PartialContractionGraph {
        PartialContractionGraph {
            nodes: &mut self.nodes[..],
            id_offset: 0
        }
    }

    fn as_first_out_graphs(self) -> ((OwnedGraph, OwnedGraph), Option<(Vec<NodeId>, Vec<NodeId>)>) {
        let (outgoing, incoming): (Vec<(Vec<Link>, Vec<NodeId>)>, Vec<(Vec<Link>, Vec<NodeId>)>) = self.nodes.into_iter()
            .map(|node| {
                (node.outgoing.into_iter().unzip(), node.incoming.into_iter().unzip())
            }).unzip();

        let (outgoing, forward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = outgoing.into_iter().unzip();
        let (incoming, backward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = incoming.into_iter().unzip();
        let forward_shortcut_middles = forward_shortcut_middles.into_iter().flat_map(|data| data.into_iter() ).collect();
        let backward_shortcut_middles = backward_shortcut_middles.into_iter().flat_map(|data| data.into_iter() ).collect();

        // currently we stick to the reordered graph and also translate the query node ids.
        // TODO make more explicit

        ((OwnedGraph::from_adjancecy_lists(outgoing), OwnedGraph::from_adjancecy_lists(incoming)), Some((forward_shortcut_middles, backward_shortcut_middles)))
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
        for &(Link { node: from, .. }, _) in node.incoming.iter() {
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(self.id_offset - 1);
        }
        for &(Link { node: to, .. }, _) in node.outgoing.iter() {
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(self.id_offset - 1);
        }
    }

    fn insert_or_decrease(&mut self, from: NodeId, to: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_or_decrease_outgoing(to, weight, over);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_or_decrease_incoming(from, weight, over);

        assert!(out_result == in_result);
        out_result
    }

    fn shortcut_required(&self, from: NodeId, to: NodeId, shortcut_weight: Weight) -> bool {
        let mut server = ::shortest_path::query::bidirectional_dijkstra::Server {
            forward_dijkstra: SteppedDijkstra::new(ForwardWrapper { graph: &self }),
            backward_dijkstra: SteppedDijkstra::new(BackwardWrapper { graph: &self }),
            tentative_distance: INFINITY,
            maximum_distance: shortcut_weight,
            meeting_node: 0
        };

        match server.distance(from - self.id_offset, to - self.id_offset) {
            Some(length) if length < shortcut_weight => false,
            Some(_) => true,
            None => true,
        }
    }
}

pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: Graph, node_order: Vec<NodeId>) -> ((OwnedGraph, OwnedGraph), Option<(Vec<NodeId>, Vec<NodeId>)>) {
    let mut graph = ContractionGraph::new(graph, node_order);
    graph.contract();
    graph.as_first_out_graphs()
}

#[derive(Debug)]
struct ForwardWrapper<'a> {
    graph: &'a PartialContractionGraph<'a>
}

#[derive(Debug)]
struct BackwardWrapper<'a> {
    graph: &'a PartialContractionGraph<'a>
}

impl<'a> Graph for ForwardWrapper<'a> {
    fn num_nodes(&self) -> usize {
        self.graph.nodes.len()
    }
}

impl<'a> Graph for BackwardWrapper<'a> {
    fn num_nodes(&self) -> usize {
        self.graph.nodes.len()
    }
}

// workaround until we get an implementation of https://github.com/rust-lang/rfcs/pull/2071
#[derive(Debug)]
struct LinkMappingIterator<'a> {
    iter: std::slice::Iter<'a, (Link, NodeId)>,
    offset: NodeId,
}

impl<'a> Iterator for LinkMappingIterator<'a> {
    type Item = Link;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(&(Link { node: target, weight }, _)) => Some(Link { node: target - self.offset, weight }),
            None => None,
        }
    }
}

use std;
impl<'a, 'b> LinkIterGraph<'b> for ForwardWrapper<'a> {
    type Iter = LinkMappingIterator<'b>;

    fn neighbor_iter(&'b self, node: NodeId) -> Self::Iter {
        LinkMappingIterator {
            iter: self.graph.nodes[node as usize].outgoing.iter(),
            offset: self.graph.id_offset
        }
    }
}

impl<'a, 'b> LinkIterGraph<'b> for BackwardWrapper<'a> {
    type Iter = LinkMappingIterator<'b>;

    fn neighbor_iter(&'b self, node: NodeId) -> Self::Iter {
        LinkMappingIterator {
            iter: self.graph.nodes[node as usize].incoming.iter(),
            offset: self.graph.id_offset
        }
    }
}
