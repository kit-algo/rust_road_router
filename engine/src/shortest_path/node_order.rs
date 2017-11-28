use super::*;

pub type Rank = NodeId;

#[derive(Debug)]
pub struct NodeOrder {
    // NodeIds ordered by their ranks - that is in ascending importance
    node_order: Vec<NodeId>,
    // The rank of each node - 0 is the lowest importance, n-1 the highest
    ranks: Vec<Rank>
}

impl NodeOrder {
    pub fn from_node_order(node_order: Vec<NodeId>) -> NodeOrder {
        let n = node_order.len();
        assert!(n < <NodeId>::max_value() as usize);
        let mut ranks = vec![n as Rank; n];

        for (i, &node) in node_order.iter().enumerate() {
            ranks[node as usize] = i as Rank;
        }

        NodeOrder { node_order, ranks }
    }

    pub fn from_ranks(ranks: Vec<Rank>) -> NodeOrder {
        let n = ranks.len();
        assert!(n < <NodeId>::max_value() as usize);
        let mut node_order = vec![n as NodeId; n];

        for (node, &rank) in ranks.iter().enumerate() {
            node_order[rank as usize] = node as NodeId;
        }

        NodeOrder { node_order, ranks }
    }

    pub fn rank(&self, node: NodeId) -> Rank {
        self.ranks[node as usize]
    }

    pub fn node(&self, rank: Rank) -> NodeId {
        self.node_order[rank as usize]
    }
}