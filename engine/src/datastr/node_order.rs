use crate::datastr::graph::*;
use crate::io::*;

pub type Rank = NodeId;

/// A type for node orders which allows efficiently retrieving both the rank in the order of a node
/// and the node for a given rank. Mostly useful, because this type makes it always clear
/// in which direction the mapping goes.
#[derive(Debug, Clone)]
pub struct NodeOrder {
    // NodeIds ordered by their ranks - that is ascending in importance
    node_order: Vec<NodeId>,
    // The rank of each node - 0 is the lowest importance, n-1 the highest
    ranks: Vec<Rank>,
}

impl NodeOrder {
    /// Create a `NodeOrder` where the id is equal to the rank
    pub fn identity(n: usize) -> NodeOrder {
        NodeOrder {
            node_order: (0..n as u32).collect(),
            ranks: (0..n as u32).collect(),
        }
    }

    /// Create a `NodeOrder` from a order vector, that is a vector containing the node ids ordered by their rank.
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

    /// Create a `NodeOrder` from a rank vector, that is a vector where `rank[id]` contains the rank for node `id`
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

    /// Get node order (rank -> node) as a slice
    pub fn order(&self) -> &[NodeId] {
        &self.node_order
    }

    /// Get node ranks (node -> rank) as a slice
    pub fn ranks(&self) -> &[NodeId] {
        &self.ranks
    }

    /// Get rank for a given node
    pub fn rank(&self, node: NodeId) -> Rank {
        self.ranks[node as usize]
    }

    /// Get node for a given rank
    pub fn node(&self, rank: Rank) -> NodeId {
        self.node_order[rank as usize]
    }

    /// Number of nodes in the order
    pub fn len(&self) -> usize {
        self.node_order.len()
    }

    /// Are there no nodes in the order?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deconstruct for NodeOrder {
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("ranks", &self.ranks)
    }
}

impl Reconstruct for NodeOrder {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        loader.load("ranks").map(Self::from_ranks)
    }
}
