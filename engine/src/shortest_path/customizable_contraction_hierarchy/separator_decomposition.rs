use super::*;

#[derive(Debug)]
pub struct SeparatorTree {
    pub nodes: Vec<NodeId>,
    pub children: Vec<SeparatorTree>,
    pub num_nodes: usize,
}

impl SeparatorTree {
    pub fn validate_for_parallelization(&self) {
        for nodes in self.nodes.windows(2) {
            assert_eq!(nodes[0], nodes[1] + 1, "Disconnected ID Ranges in nested dissection separator")
        }

        let mut child_range_start = self.nodes.first().cloned().unwrap_or(self.num_nodes as u32 - 1) + 1 - self.num_nodes as u32;
        for child in &self.children {
            assert_eq!(child_range_start, child.nodes[0] + 1 - child.num_nodes as u32, "Disconnected ID Ranges in nested dissection cells");
            child_range_start += child.num_nodes as u32;
        }

        for children in self.children.windows(2) {
            assert!(children[0].num_nodes >= children[1].num_nodes, "Sub cells not sorted by descending size");
        }

        for child in &self.children {
            child.validate_for_parallelization();
        }
    }

    pub fn new(cch: &CCH) -> Self {
        let mut children = vec![Vec::new(); cch.num_nodes()];
        let mut roots = Vec::new();

        for (node_index, parent) in cch.elimination_tree.iter().enumerate() {
            if let Some(parent) = parent.value() {
                children[parent as usize].push(node_index as NodeId);
            } else {
                roots.push(node_index as NodeId);
            }
        }

        let children: Vec<_> = roots.into_iter().map(|root| SeparatorTree::new_subtree(root, &children)).collect();
        let num_nodes = children.iter().map(|child| child.num_nodes).sum::<usize>();
        debug_assert_eq!(num_nodes, cch.num_nodes());

        SeparatorTree { nodes: Vec::new(), children, num_nodes }
    }

    fn new_subtree(root: NodeId, children: &[Vec<NodeId>]) -> SeparatorTree {
        let mut node = root;
        let mut nodes = vec![root];

        #[allow(clippy::match_ref_pats)]
        while let &[only_child] = &children[node as usize][..] {
            node = only_child;
            nodes.push(only_child)
        }

        let children: Vec<_> = children[node as usize].iter().map(|&child| { debug_assert!(child < node); Self::new_subtree(child, children) }).collect();
        let num_nodes = nodes.len() + children.iter().map(|child| child.num_nodes).sum::<usize>();

        debug_assert!(num_nodes > 0);
        debug_assert!(!nodes.is_empty());

        SeparatorTree {
            nodes,
            children,
            num_nodes,
        }
    }
}
