use super::*;
use self::adjacency_lists_graph::AdjacencyListsGraph as Graph;

pub fn contract(graph: &mut Graph, node_ranks: &Vec<u32>) {
    let mut order: Vec<NodeId> = vec![0; graph.num_nodes()];

    for (node, &rank) in node_ranks.iter().enumerate() {
        order[rank as usize] = node as NodeId;
    }

    for &node in order.iter() {
        // attack of the borrow checker!!!
        // usually we would just iterate over outgoing and incoming edges in two nested for loops and inside we would insert shortcuts
        // problem: the iterations borrow the graph and the shortcut insertion wants to borrow mutable - which the borrow checker does not permit
        // so we need to temporarily store something and hope the compiler will figure it out
        // or get better with rust

        let upper_incoming: Vec<Link> = graph.incmoing_iter(node).filter(|&&Link { node: other, .. }| node_ranks[other as usize] > node_ranks[node as usize]).cloned().collect();
        let upper_outgoing: Vec<Link> = graph.outgoing_iter(node).filter(|&&Link { node: other, .. }| node_ranks[other as usize] > node_ranks[node as usize]).cloned().collect();

        for &Link { node: from, weight: from_weight } in upper_incoming.iter() {
            for &Link { node: to, weight: to_weight } in upper_outgoing.iter() {
                if shortcut_required(graph, from, to, from_weight + to_weight) {
                    graph.insert_or_decrease(from, to, from_weight + to_weight);
                }
            }
        }
    }
}

fn shortcut_required(_graph: &Graph, _from: NodeId, _to: NodeId, _weight: Weight) -> bool {
    true
}
