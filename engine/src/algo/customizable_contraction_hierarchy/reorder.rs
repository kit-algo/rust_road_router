//! Modify CCH nested dissection orders.
//!
//! FlowCutter emits orders of the form sep0-sep1l-sep1r-rest1l-rest1r
//! But we need sep0-sep1l-rest1l-sep1r-rest1r - cells must always form a consecutive node id block including their separator.

use super::*;
use crate::util::NonNan;

/// Supplemental data for reordering operations
#[derive(Debug)]
pub struct CCHReordering<'a, 'c> {
    pub cch: &'c CCH,
    pub latitude: &'a [f32],
    pub longitude: &'a [f32],
}

impl<'a, 'c> CCHReordering<'a, 'c> {
    fn distance(&self, n1: NodeId, n2: NodeId) -> NonNan {
        use nav_types::WGS84;
        NonNan::new(
            WGS84::new(
                self.latitude[self.cch.node_order.node(n1) as usize],
                self.longitude[self.cch.node_order.node(n1) as usize],
                0.0,
            )
            .distance(&WGS84::new(
                self.latitude[self.cch.node_order.node(n2) as usize],
                self.longitude[self.cch.node_order.node(n2) as usize],
                0.0,
            )),
        )
        .unwrap()
    }

    fn reorder_sep(&self, nodes: &mut [NodeId]) {
        let furthest = nodes
            .first()
            .map(|&first| nodes.iter().max_by_key(|&&node| self.distance(first, node)).unwrap());

        if let Some(&furthest) = furthest {
            nodes.sort_by_key(|&node| self.distance(node, furthest))
        }
    }

    fn reorder_tree(&self, separators: &mut SeparatorTree, level: usize) {
        if level > 2 {
            return;
        }

        self.reorder_sep(&mut separators.nodes);
        for child in &mut separators.children {
            self.reorder_tree(child, level + 1);
            // if let Some(&first) = child.nodes.first() {
            //     if let Some(&last) = child.nodes.last() {
            //         if let Some(&node) = separators.nodes.first() {
            //             if self.distance(first, node) < self.distance(last, node) {
            //                 child.nodes.reverse()
            //             }
            //         }
            //     }
            // }
        }
    }

    fn reorder_children_by_size(&self, separators: &mut SeparatorTree) {
        separators.children.sort_unstable_by_key(|c| c.num_nodes);

        for child in &mut separators.children {
            self.reorder_children_by_size(child);
        }
    }

    fn to_ordering(&self, seperators: SeparatorTree, order: &mut Vec<NodeId>) {
        order.extend(seperators.nodes);
        for child in seperators.children {
            self.to_ordering(child, order);
        }
    }

    /// Try to reorder separator nodes of top levels such that they follow a linear order based on their geographic position
    pub fn reorder(&self) -> NodeOrder {
        let mut separators = self.cch.separators();
        self.reorder_tree(&mut separators, 0);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.cch.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }

    /// Create a new order which has the same separator tree but is laid out, such that the separator based paralellization works fine.
    pub fn reorder_for_seperator_based_customization(&self) -> NodeOrder {
        let mut separators = self.cch.separators();
        self.reorder_children_by_size(&mut separators);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.cch.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }
}
