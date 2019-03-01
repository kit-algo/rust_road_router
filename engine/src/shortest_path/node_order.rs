use super::*;
use crate::io::*;

pub type Rank = NodeId;

#[derive(Debug, Clone)]
pub struct NodeOrder {
    // NodeIds ordered by their ranks - that is ascending in importance
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

        debug_assert_eq!(ranks.iter().position(|&rank| rank == n as Rank), None);

        NodeOrder { node_order, ranks }
    }

    pub fn from_ranks(ranks: Vec<Rank>) -> NodeOrder {
        let n = ranks.len();
        assert!(n < <NodeId>::max_value() as usize);
        let mut node_order = vec![n as NodeId; n];

        for (node, &rank) in ranks.iter().enumerate() {
            node_order[rank as usize] = node as NodeId;
        }

        debug_assert_eq!(node_order.iter().position(|&node| node == n as NodeId), None);

        NodeOrder { node_order, ranks }
    }

    pub fn rank(&self, node: NodeId) -> Rank {
        self.ranks[node as usize]
    }

    pub fn node(&self, rank: Rank) -> NodeId {
        self.node_order[rank as usize]
    }

    pub fn len(&self) -> usize {
        self.node_order.len()
    }
}

impl Deconstruct for NodeOrder {
    fn store_each(&self, store: &Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("ranks", &self.ranks)
    }
}

impl Reconstruct for NodeOrder {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        loader.load("ranks").map(Self::from_ranks)
    }
}
