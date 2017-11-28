use super::*;
use self::first_out_graph::FirstOutGraph;

pub mod ch_graph;

#[derive(Debug, PartialEq)]
enum ShortcutResult {
    NewShortcut,
    Existed
}

#[derive(Debug)]
struct Node {
    outgoing: Vec<NodeId>,
    incoming: Vec<NodeId>
}

impl Node {
    fn insert_outgoing(&mut self, to: NodeId) -> ShortcutResult {
        Node::insert(&mut self.outgoing, to)
    }

    fn insert_incoming(&mut self, from: NodeId) -> ShortcutResult {
        Node::insert(&mut self.incoming, from)
    }

    fn insert(links: &mut Vec<NodeId>, node: NodeId) -> ShortcutResult {
        for &other in links.iter() {
            if node == other {
                return ShortcutResult::Existed
            }
        }

        links.push(node);
        ShortcutResult::NewShortcut
    }

    fn remove_outgoing(&mut self, to: NodeId) {
        let pos = self.outgoing.iter().position(|&node| to == node).unwrap();
        self.outgoing.swap_remove(pos);
    }

    fn remove_incmoing(&mut self, from: NodeId) {
        let pos = self.incoming.iter().position(|&node| from == node).unwrap();
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
            let outs = (0..n).map(|node|
                graph.neighbor_iter(node_order[node])
                    .map(|Link { node, .. }| node_ranks[node as usize])
                    .collect()
            );
            let reversed = graph.reverse();
            let ins = (0..n).map(|node|
                reversed.neighbor_iter(node_order[node])
                    .map(|Link { node, .. }| node_ranks[node as usize])
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
            for &from in node.incoming.iter() {
                for &to in node.outgoing.iter() {
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
        // let (outgoing, incoming): (Vec<(Vec<Link>, Vec<NodeId>)>, Vec<(Vec<Link>, Vec<NodeId>)>) = self.nodes.into_iter()
        //     .map(|node| {
        //         (node.outgoing.into_iter().unzip(), node.incoming.into_iter().unzip())
        //     }).unzip();

        // let (outgoing, forward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = outgoing.into_iter().unzip();
        // let (incoming, backward_shortcut_middles): (Vec<Vec<Link>>, Vec<Vec<NodeId>>) = incoming.into_iter().unzip();
        // let forward_shortcut_middles = forward_shortcut_middles.into_iter().flat_map(|data| data.into_iter() ).collect();
        // let backward_shortcut_middles = backward_shortcut_middles.into_iter().flat_map(|data| data.into_iter() ).collect();

        // // currently we stick to the reordered graph and also translate the query node ids.
        // // TODO make more explicit

        // ((FirstOutGraph::from_adjancecy_lists(outgoing), FirstOutGraph::from_adjancecy_lists(incoming)), None)
        unimplemented!()
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
        for &from in node.incoming.iter() {
            self.nodes[(from - self.id_offset) as usize].remove_outgoing(self.id_offset - 1);
        }
        for &to in node.outgoing.iter() {
            self.nodes[(to - self.id_offset) as usize].remove_incmoing(self.id_offset - 1);
        }
    }

    fn insert(&mut self, from: NodeId, to: NodeId) -> ShortcutResult {
        let out_result = self.nodes[(from - self.id_offset) as usize].insert_outgoing(to);
        let in_result = self.nodes[(to - self.id_offset) as usize].insert_incoming(from);

        assert!(out_result == in_result);
        out_result
    }
}

pub fn contract(graph: FirstOutGraph, node_order: Vec<NodeId>) -> ((FirstOutGraph, FirstOutGraph), Option<(Vec<NodeId>, Vec<NodeId>)>) {
    let mut graph = ContractionGraph::new(graph, node_order);
    graph.contract();
    graph.as_first_out_graphs()
}
