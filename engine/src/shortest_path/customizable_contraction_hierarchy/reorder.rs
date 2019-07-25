use super::*;
use crate::util::NonNan;

#[derive(Debug)]
pub struct CCHReordering<'a> {
    pub node_order: NodeOrder,
    pub latitude: &'a [f32],
    pub longitude: &'a [f32],
}

impl<'a> CCHReordering<'a> {
    fn distance (&self, n1: NodeId, n2: NodeId) -> NonNan {
        use nav_types::WGS84;
        NonNan::new(WGS84::new(self.latitude[self.node_order.node(n1) as usize], self.longitude[self.node_order.node(n1) as usize], 0.0)
            .distance(&WGS84::new(self.latitude[self.node_order.node(n2) as usize], self.longitude[self.node_order.node(n2) as usize], 0.0))).unwrap()
    }

    fn reorder_sep(&self, nodes: &mut [NodeId]) {
        let furthest = nodes.first().map(|&first| {
            nodes.iter().max_by_key(|&&node| self.distance(first, node)).unwrap()
        });

        if let Some(&furthest) = furthest {
            nodes.sort_by_key(|&node| self.distance(node, furthest))
        }
    }

    fn reorder_tree(&self, separators: &mut SeparatorTree, level: usize) {
        if level > 2 { return }

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

    pub fn reorder(self, mut separators: SeparatorTree) -> NodeOrder {
        self.reorder_tree(&mut separators, 0);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }

    pub fn reorder_for_seperator_based_customization(self, mut separators: SeparatorTree) -> NodeOrder {
        self.reorder_children_by_size(&mut separators);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }
}
