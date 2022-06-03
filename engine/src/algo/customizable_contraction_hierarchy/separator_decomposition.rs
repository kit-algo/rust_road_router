//! Reconstruct separator tree from nested dissection order and elimination tree.

use super::*;

#[derive(Clone)]
pub enum SeparatorNodes {
    Consecutive(Range<NodeId>),
    Random(Vec<NodeId>),
}

impl SeparatorNodes {
    fn new() -> Self {
        SeparatorNodes::Consecutive(0..0)
    }

    fn with_one_node(node: NodeId) -> Self {
        SeparatorNodes::Consecutive(node..node + 1)
    }

    fn push(&mut self, node: NodeId) {
        match self {
            SeparatorNodes::Consecutive(range) => {
                if range.start > 0 && node == range.start - 1 {
                    range.start -= 1;
                } else {
                    *self = SeparatorNodes::Random(Vec::from_iter(range.rev().chain(std::iter::once(node))));
                }
            }
            SeparatorNodes::Random(nodes) => {
                nodes.push(node);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            SeparatorNodes::Random(nodes) => nodes.is_empty(),
            SeparatorNodes::Consecutive(range) => range.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SeparatorNodes::Random(nodes) => nodes.len(),
            SeparatorNodes::Consecutive(range) => range.len(),
        }
    }

    fn highest_ranked_node(&self) -> Option<NodeId> {
        match self {
            SeparatorNodes::Random(nodes) => nodes.first().copied(),
            SeparatorNodes::Consecutive(range) => range.clone().rev().next(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
        match self {
            SeparatorNodes::Consecutive(range) => EitherOrIterator::Either(range.clone().rev()),
            SeparatorNodes::Random(nodes) => EitherOrIterator::Or(nodes.iter().copied()),
        }
    }

    pub fn mut_nodes(&mut self) -> &mut [NodeId] {
        if let SeparatorNodes::Consecutive(range) = self {
            *self = SeparatorNodes::Random(Vec::from_iter(range.rev()));
        }

        match self {
            SeparatorNodes::Random(nodes) => nodes,
            SeparatorNodes::Consecutive(_) => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct SeparatorTree {
    pub nodes: SeparatorNodes,
    pub children: Vec<SeparatorTree>,
    pub num_nodes: usize,
}

impl SeparatorTree {
    /// Check if the node order used for the CCH allows for safe basic parellized customization.
    pub fn validate_for_parallelization(&self) {
        assert!(
            matches!(&self.nodes, SeparatorNodes::Consecutive(_)),
            "Disconnected ID Ranges in nested dissection separator"
        );

        let mut child_range_start = self.nodes.highest_ranked_node().unwrap_or(self.num_nodes as u32 - 1) + 1 - self.num_nodes as u32;
        for child in &self.children {
            assert_eq!(
                child_range_start,
                child.nodes.highest_ranked_node().unwrap() + 1 - child.num_nodes as u32,
                "Disconnected ID Ranges in nested dissection cells"
            );
            child_range_start += child.num_nodes as u32;
        }

        for children in self.children.windows(2) {
            assert!(children[0].num_nodes >= children[1].num_nodes, "Sub cells not sorted by descending size");
        }

        for child in &self.children {
            child.validate_for_parallelization();
        }
    }

    /// Reconstruct separator tree from a preprocessed CCH
    pub fn new(elimination_tree: &[InRangeOption<NodeId>]) -> Self {
        let mut children = vec![Children::None; elimination_tree.len()];
        let mut roots = Vec::new();

        for (node_index, parent) in elimination_tree.iter().enumerate() {
            if let Some(parent) = parent.value() {
                let children = &mut children[parent as usize];
                match children {
                    Children::None => *children = Children::One(node_index as NodeId),
                    Children::One(child) => *children = Children::Many(vec![*child, node_index as NodeId]),
                    Children::Many(children) => children.push(node_index as NodeId),
                }
            } else {
                roots.push(node_index as NodeId);
            }
        }

        let children: Vec<_> = roots.into_iter().map(|root| SeparatorTree::new_subtree(root, &children)).collect();
        let num_nodes = children.iter().map(|child| child.num_nodes).sum::<usize>();
        debug_assert_eq!(num_nodes, elimination_tree.len());

        SeparatorTree {
            nodes: SeparatorNodes::new(),
            children,
            num_nodes,
        }
    }

    fn new_subtree(root: NodeId, children: &[Children]) -> SeparatorTree {
        let mut node = root;
        let mut nodes = SeparatorNodes::with_one_node(root);

        while let &Children::One(only_child) = &children[node as usize] {
            node = only_child;
            nodes.push(only_child)
        }

        let children: Vec<_> = match &children[node as usize] {
            Children::None => vec![],
            Children::Many(cells) => cells
                .iter()
                .map(|&child| {
                    debug_assert!(child < node);
                    Self::new_subtree(child, children)
                })
                .collect(),
            Children::One(_) => unreachable!(),
        };
        let num_nodes = nodes.len() + children.iter().map(|child| child.num_nodes).sum::<usize>();

        debug_assert!(num_nodes > 0);
        debug_assert!(!nodes.is_empty());

        SeparatorTree { nodes, children, num_nodes }
    }
}

#[derive(Clone)]
enum Children {
    None,
    One(NodeId),
    Many(Vec<NodeId>),
}
