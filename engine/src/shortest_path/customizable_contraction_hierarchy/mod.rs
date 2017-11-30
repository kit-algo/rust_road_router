use std;
use super::*;
use super::first_out_graph::FirstOutGraph;
use shortest_path::node_order::NodeOrder;
use ::inrange_option::InrangeOption;

pub mod ch_graph;

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
    }

    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&(node, _)| from == node).unwrap();
        self.incoming.swap_remove(pos);
    }
}

#[derive(Debug)]
struct ContractionGraph {
    nodes: Vec<Node>,
    node_order: NodeOrder
}

impl ContractionGraph {
    fn new(graph: FirstOutGraph, node_order: NodeOrder) -> ContractionGraph {
        let n = graph.num_nodes() as NodeId;

        let nodes = {
            let outs: Vec<Vec<(NodeId, InrangeOption<EdgeId>)>> = (0..n).map(|node|
                graph.neighbor_iter(node_order.node(node))
                    .map(|Link { node, .. }| node_order.rank(node))
                    .zip(graph.neighbor_edge_indices(node).map(|edge_id| InrangeOption::new(Some(edge_id))))
                    .collect()
            ).collect();

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
            node_order
        }
    }

    fn contract(&mut self) {
        let mut graph = self.partial_graph();

        while let Some((node, mut subgraph)) = graph.remove_lowest() {
            for &(from, _) in node.incoming.iter() {
                for &(to, _) in node.outgoing.iter() {
                    subgraph.insert(from, to);
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

    fn as_first_out_graphs(self) -> ((FirstOutGraph, FirstOutGraph), Option<(Vec<NodeId>, Vec<NodeId>)>) {
        let (outgoing, incoming) = self.nodes.into_iter().map(|node| {
            (node.outgoing.into_iter().map(|(node, _)| node).collect(), node.incoming.into_iter().map(|(node, _)| node).collect())
        }).unzip();

        ((Self::adjancecy_lists_to_first_out_graph(outgoing), Self::adjancecy_lists_to_first_out_graph(incoming)), None)
    }

    fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Vec<NodeId>>) -> FirstOutGraph {
        let n = adjancecy_lists.len();
        // create first_out array by doing a prefix sum over the adjancecy list sizes
        let first_out: Vec<u32> = std::iter::once(0).chain(adjancecy_lists.iter().scan(0, |state, incoming_links| {
            *state = *state + incoming_links.len() as u32;
            Some(*state)
        })).collect();
        debug_assert_eq!(first_out.len(), n + 1);

        // append all adjancecy list and split the pairs into two seperate vectors
        let head: Vec<NodeId> = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter())
            .collect();

        let m = head.len();
        FirstOutGraph::new(first_out, head, vec![INFINITY; m])
    }

    fn elimination_trees(&self) -> Vec<InrangeOption<NodeId>> {
        let n = self.nodes.len();
        let mut elimination_tree = vec![InrangeOption::new(None); n];

        for (rank, node) in self.nodes.iter().enumerate() {
            elimination_tree[rank] = InrangeOption::new(node.outgoing.iter().chain(node.incoming.iter()).map(|&(node, _)| node).min());
            debug_assert!(elimination_tree[rank].value().unwrap_or(n as NodeId) as usize > rank);
        }

        elimination_tree
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
        for &(from, _) in node.incoming.iter() {
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(self.id_offset - 1);
        }
        for &(to, _) in node.outgoing.iter() {
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(self.id_offset - 1);
        }
    }

    fn insert(&mut self, from: NodeId, to: NodeId) -> ShortcutResult {
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_outgoing(to);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_incoming(from);

        assert_eq!(out_result, in_result);
        out_result
    }
}

pub fn contract(graph: FirstOutGraph, node_order: NodeOrder) -> ((FirstOutGraph, FirstOutGraph), Option<(Vec<NodeId>, Vec<NodeId>)>) {
    let mut graph = ContractionGraph::new(graph, node_order);
    graph.contract();
    graph.as_first_out_graphs()
}
