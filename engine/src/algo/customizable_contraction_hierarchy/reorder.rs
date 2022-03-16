//! Modify CCH nested dissection orders.
//!
//! FlowCutter emits orders of the form sep0-sep1l-sep1r-rest1l-rest1r
//! But we need sep0-sep1l-rest1l-sep1r-rest1r - cells must always form a consecutive node id block including their separator.

use super::*;
use crate::util::NonNan;

fn distance(n1: NodeId, n2: NodeId, latitude: &[f32], longitude: &[f32]) -> NonNan {
    let delta_lat = latitude[n1 as usize] - latitude[n2 as usize];
    let delta_lng = longitude[n1 as usize] - longitude[n2 as usize];

    NonNan::new(delta_lat * delta_lat + delta_lng * delta_lng).unwrap()
}

fn reorder_sep(nodes: &mut [NodeId], order: &NodeOrder, latitude: &[f32], longitude: &[f32]) {
    let furthest = nodes.first().map(|&first| {
        nodes
            .iter()
            .max_by_key(|&&node| distance(order.node(first), order.node(node), latitude, longitude))
            .unwrap()
    });

    if let Some(&furthest) = furthest {
        nodes.sort_by_key(|&node| distance(order.node(node), order.node(furthest), latitude, longitude))
    }
}

fn reorder_tree(separators: &mut SeparatorTree, level: usize, order: &NodeOrder, latitude: &[f32], longitude: &[f32]) {
    if level > 2 {
        return;
    }

    reorder_sep(&mut separators.nodes, order, latitude, longitude);
    for child in &mut separators.children {
        reorder_tree(child, level + 1, order, latitude, longitude);
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

fn reorder_children_by_size(separators: &mut SeparatorTree) {
    separators.children.sort_unstable_by_key(|c| c.num_nodes);

    for child in &mut separators.children {
        reorder_children_by_size(child);
    }
}

fn to_ordering(seperators: &SeparatorTree, order: &mut Vec<NodeId>) {
    order.extend(&seperators.nodes);
    for child in &seperators.children {
        to_ordering(child, order);
    }
}

/// Try to reorder separator nodes of top levels such that they follow a linear order based on their geographic position
pub fn reorder(cch: &CCH, latitude: &[f32], longitude: &[f32]) -> NodeOrder {
    let mut separators = cch.separators().clone();
    reorder_tree(&mut separators, 0, cch.node_order(), latitude, longitude);
    let mut order = Vec::new();
    to_ordering(&separators, &mut order);

    for rank in &mut order {
        *rank = cch.node_order.node(*rank);
    }
    order.reverse();

    NodeOrder::from_node_order(order)
}

/// Create a new order which has the same separator tree but is laid out, such that the separator based paralellization works fine.
pub fn reorder_for_seperator_based_customization(cch_order: &NodeOrder, mut separators: SeparatorTree) -> NodeOrder {
    reorder_children_by_size(&mut separators);
    let mut order = Vec::new();
    to_ordering(&separators, &mut order);

    for rank in &mut order {
        *rank = cch_order.node(*rank);
    }
    order.reverse();

    NodeOrder::from_node_order(order)
}

pub fn reorder_bfs(cch: &CCH) -> NodeOrder {
    let separators = cch.separators();
    let mut order = Vec::new();

    let mut queue = std::collections::VecDeque::new();
    queue.push_back(separators);

    while let Some(sep) = queue.pop_front() {
        order.extend_from_slice(&sep.nodes);
        for child in &sep.children {
            queue.push_back(child);
        }
    }

    for rank in &mut order {
        *rank = cch.node_order.node(*rank);
    }
    order.reverse();

    NodeOrder::from_node_order(order)
}
