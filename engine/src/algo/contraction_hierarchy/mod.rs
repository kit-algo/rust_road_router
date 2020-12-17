//! Experimental prototype implementation of Contraction Hierarchies in rust.
//!
//! Not tuned for performance yet.
//! No node ordering implemented yet, depends on getting a precalculated order.

use super::*;
use crate::algo::dijkstra::generic_dijkstra::*;
use crate::datastr::node_order::NodeOrder;

pub mod query;

/// Struct for a Contraction Hierarchy, that is the completely preprocessed
/// graph augmented by shortcuts and split in an upwards and downward part,
/// optionally with unpacking info.
pub struct ContractionHierarchy {
    forward: OwnedGraph,
    backward: OwnedGraph,
    middle_nodes: Option<(Vec<NodeId>, Vec<NodeId>)>,
}

impl ContractionHierarchy {
    /// Create CH struct from augmented graph and node order.
    pub fn from_contracted_graph(graph: OwnedGraph, order: &NodeOrder) -> ContractionHierarchy {
        let (forward, backward) = graph.ch_split(order);
        ContractionHierarchy {
            forward,
            backward,
            middle_nodes: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    ShortenedExisting,
    ShorterExisting,
}

/// Struct for nodes during contraction.
/// Allows adding, removing or updating edges.
#[derive(Debug)]
struct Node {
    outgoing: Vec<(Link, NodeId)>,
    incoming: Vec<(Link, NodeId)>,
}

impl Node {
    fn insert_or_decrease_outgoing(&mut self, to: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.outgoing, to, weight, over)
    }

    fn insert_or_decrease_incoming(&mut self, from: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        Node::insert_or_decrease(&mut self.incoming, from, weight, over)
    }

    fn insert_or_decrease(links: &mut Vec<(Link, NodeId)>, node: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        for &mut (
            Link {
                node: other,
                weight: ref mut other_weight,
            },
            ref mut shortcut_middle,
        ) in links.iter_mut()
        {
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

    // remove links to a specific other node
    fn remove_outgoing(&mut self, to: NodeId) {
        let pos = self.outgoing.iter().position(|&(Link { node, .. }, _)| to == node).unwrap();
        self.outgoing.swap_remove(pos);
    }

    // remove links from a specific other node
    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&(Link { node, .. }, _)| from == node).unwrap();
        self.incoming.swap_remove(pos);
    }
}

/// Intermediate graph representation for during the preprocessing.
/// During contraction, we need a dynamic graph, so we use a `Vec` of nodes instead of the typical adjacency array structure.
#[derive(Debug)]
struct ContractionGraph {
    nodes: Vec<Node>,
    order: NodeOrder,
}

impl ContractionGraph {
    // Create a ContractionGraph from a regular graph and an order.
    fn new<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, order: NodeOrder) -> ContractionGraph {
        let n = graph.num_nodes();

        // We need to:
        // - filter out loops
        // - translate the node ids
        // - create the struct we use during preprocessing
        let nodes = {
            let outs = (0..n).map(|node| {
                graph
                    .link_iter(order.node(node as NodeId))
                    .filter(|&Link { node: head, .. }| head != order.node(node as NodeId))
                    .map(|Link { node, weight }| {
                        (
                            Link {
                                node: order.rank(node),
                                weight,
                            },
                            n as NodeId,
                        )
                    })
                    .collect()
            });
            let reversed = OwnedGraph::reversed(graph);
            let ins = (0..n).map(|node| {
                LinkIterable::<Link>::link_iter(&reversed, order.node(node as NodeId))
                    .filter(|&Link { node: head, .. }| head != order.node(node as NodeId))
                    .map(|Link { node, weight }| {
                        (
                            Link {
                                node: order.rank(node),
                                weight,
                            },
                            n as NodeId,
                        )
                    })
                    .collect()
            });
            outs.zip(ins).map(|(outgoing, incoming)| Node { outgoing, incoming }).collect()
        };

        ContractionGraph { nodes, order }
    }

    // contract all nodes - full preprocessing
    fn contract(&mut self) {
        self.contract_partially(self.nodes.len())
    }

    // contract a fixed number of nodes
    fn contract_partially(&mut self, mut contraction_count: usize) {
        // We utilize split borrows to make node contraction work well with rusts borrowing rules.
        // The graph representation already contains the node in order of increasing rank.
        // We iteratively split of the lowest ranked node.
        // This is the one that will be contracted next.
        // Contraction does not require mutation of the current node,
        // but we need to insert shortcuts between nodes of higher rank.

        // we start with the complete graph
        let mut graph = self.partial_graph();
        // witness search servers with recycling for reduced allocations
        let mut recycled = (
            GenericDijkstra::<ForwardWrapper>::new(ForwardWrapper { graph: &graph }).recycle(),
            GenericDijkstra::<BackwardWrapper>::new(BackwardWrapper { graph: &graph }).recycle(),
        );

        // split of the lowest node, the one that will be contracted
        while let Some((node, mut subgraph)) = graph.remove_lowest() {
            if contraction_count == 0 {
                break;
            } else {
                contraction_count -= 1;
            }
            // for all pairs of neighbors
            for &(Link { node: from, weight: from_wght }, _) in &node.incoming {
                for &(Link { node: to, weight: to_wght }, _) in &node.outgoing {
                    // do witness search to check if we need the shortcut
                    let (shortcut_required, new_recycled) = subgraph.shortcut_required(from, to, from_wght + to_wght, recycled);
                    recycled = new_recycled;
                    if shortcut_required {
                        let middle_node_id = subgraph.id_offset - 1;
                        // insert shortcut
                        subgraph.insert_or_decrease(from, to, from_wght + to_wght, middle_node_id);
                    }
                }
            }

            // set graph to subgraph, so we continue with the next node in the next iteration
            graph = subgraph;
        }
    }

    // create partial graph with all nodes
    fn partial_graph(&mut self) -> PartialContractionGraph {
        PartialContractionGraph {
            nodes: &mut self.nodes[..],
            id_offset: 0,
        }
    }

    // build up CH struct after contraction
    fn into_first_out_graphs(self) -> ContractionHierarchy {
        let (outgoing, incoming): (Vec<_>, Vec<_>) = self
            .nodes
            .into_iter()
            .map(|node| (node.outgoing.into_iter().unzip(), node.incoming.into_iter().unzip()))
            .unzip();

        let (outgoing, forward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = outgoing.into_iter().unzip();
        let (incoming, backward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = incoming.into_iter().unzip();
        let forward_shortcut_middles = forward_shortcut_middles.into_iter().flat_map(|data| data.into_iter()).collect();
        let backward_shortcut_middles = backward_shortcut_middles.into_iter().flat_map(|data| data.into_iter()).collect();

        ContractionHierarchy {
            forward: OwnedGraph::from_adjancecy_lists(outgoing),
            backward: OwnedGraph::from_adjancecy_lists(incoming),
            middle_nodes: Some((forward_shortcut_middles, backward_shortcut_middles)),
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
    // split of the lowest node and remove any edges from higher ranked nodes to this one
    fn remove_lowest(self) -> Option<(&'a Node, PartialContractionGraph<'a>)> {
        if let Some((node, other_nodes)) = self.nodes.split_first_mut() {
            let mut subgraph = PartialContractionGraph {
                nodes: other_nodes,
                id_offset: self.id_offset + 1,
            };
            subgraph.remove_edges_to_removed(&node);
            Some((node, subgraph))
        } else {
            None
        }
    }

    fn remove_edges_to_removed(&mut self, node: &Node) {
        for &(Link { node: from, .. }, _) in &node.incoming {
            debug_assert!(from >= self.id_offset, "{}, {}", from, self.id_offset);
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(self.id_offset - 1);
        }
        for &(Link { node: to, .. }, _) in &node.outgoing {
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(self.id_offset - 1);
        }
    }

    fn insert_or_decrease(&mut self, from: NodeId, to: NodeId, weight: Weight, over: NodeId) -> ShortcutResult {
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_or_decrease_outgoing(to, weight, over);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_or_decrease_incoming(from, weight, over);

        assert!(out_result == in_result);
        out_result
    }

    fn shortcut_required(
        &self,
        from: NodeId,
        to: NodeId,
        shortcut_weight: Weight,
        recycled: (Trash<Weight>, Trash<Weight>),
    ) -> (bool, (Trash<Weight>, Trash<Weight>)) {
        // no loop shortcuts ever required
        if from == to {
            return (false, recycled);
        }

        // create server from recycled stuff
        let mut server = crate::algo::dijkstra::query::bidirectional_dijkstra::Server {
            forward_dijkstra: GenericDijkstra::from_recycled(ForwardWrapper { graph: &self }, recycled.0),
            backward_dijkstra: GenericDijkstra::from_recycled(BackwardWrapper { graph: &self }, recycled.1),
            tentative_distance: INFINITY,
            meeting_node: 0,
        };

        // witness search is a bidirection dijkstra capped to the length of the path over the contracted node
        let res = match server.distance_with_cap(from - self.id_offset, to - self.id_offset, shortcut_weight) {
            Some(length) if length < shortcut_weight => false,
            Some(_) => true,
            None => true,
        };

        (res, (server.forward_dijkstra.recycle(), server.backward_dijkstra.recycle()))
    }
}

/// Create an overlay graph by contracting a fixed number of nodes
pub fn overlay<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, order: NodeOrder, contraction_count: usize) -> (OwnedGraph, OwnedGraph) {
    let mut graph = ContractionGraph::new(graph, order);
    graph.contract_partially(contraction_count);
    let ch = graph.into_first_out_graphs();
    (ch.forward, ch.backward)
}

/// Perform CH Preprocessing
pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, order: NodeOrder) -> ContractionHierarchy {
    let mut graph = ContractionGraph::new(graph, order);
    graph.contract();
    graph.into_first_out_graphs()
}

// Utilities for witness search

#[derive(Debug)]
struct ForwardWrapper<'a> {
    graph: &'a PartialContractionGraph<'a>,
}

#[derive(Debug)]
struct BackwardWrapper<'a> {
    graph: &'a PartialContractionGraph<'a>,
}

impl<'a> Graph for ForwardWrapper<'a> {
    fn num_nodes(&self) -> usize {
        self.graph.nodes.len()
    }

    fn num_arcs(&self) -> usize {
        unimplemented!()
    }

    fn degree(&self, _node: NodeId) -> usize {
        unimplemented!()
    }
}

impl<'a> Graph for BackwardWrapper<'a> {
    fn num_nodes(&self) -> usize {
        self.graph.nodes.len()
    }

    fn num_arcs(&self) -> usize {
        unimplemented!()
    }

    fn degree(&self, _node: NodeId) -> usize {
        unimplemented!()
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
            Some(&(Link { node: target, weight }, _)) => Some(Link {
                node: target - self.offset,
                weight,
            }),
            None => None,
        }
    }
}

impl<'a, 'b> LinkIterable<'b, Link> for ForwardWrapper<'a> {
    type Iter = LinkMappingIterator<'b>;

    fn link_iter(&'b self, node: NodeId) -> Self::Iter {
        LinkMappingIterator {
            iter: self.graph.nodes[node as usize].outgoing.iter(),
            offset: self.graph.id_offset,
        }
    }
}

impl<'a, 'b> LinkIterable<'b, Link> for BackwardWrapper<'a> {
    type Iter = LinkMappingIterator<'b>;

    fn link_iter(&'b self, node: NodeId) -> Self::Iter {
        LinkMappingIterator {
            iter: self.graph.nodes[node as usize].incoming.iter(),
            offset: self.graph.id_offset,
        }
    }
}
