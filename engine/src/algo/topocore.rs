use super::*;
use crate::algo::dijkstra::gen_topo_dijkstra::{SymmetricDeg, SymmetricDegreeGraph};
use crate::datastr::node_order::NodeOrder;
use crate::datastr::rank_select_map::*;
use crate::report::*;
use crate::util::{in_range_option::InRangeOption, Bool, TapOps};
use std::cmp::{max, min};

#[derive(Debug)]
pub struct Topocore {
    pub forward: OwnedGraph,
    pub backward: OwnedGraph,
    pub order: NodeOrder,
    pub core_size: usize,
}

#[derive(Debug)]
struct UndirectedGraph<'a, G, H> {
    ins: &'a G,
    outs: &'a H,
}

impl<'a, G: Graph, H> Graph for UndirectedGraph<'a, G, H> {
    fn num_nodes(&self) -> usize {
        self.ins.num_nodes()
    }

    fn num_arcs(&self) -> usize {
        unimplemented!()
    }

    fn degree(&self, _node: NodeId) -> usize {
        unimplemented!()
    }
}

impl<'b, L, G: LinkIterable<L>, H: LinkIterable<L>> LinkIterable<L> for UndirectedGraph<'b, G, H> {
    type Iter<'a> = std::iter::Chain<G::Iter<'a>, H::Iter<'a>>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.ins.link_iter(node).chain(self.outs.link_iter(node))
    }
}

#[allow(clippy::cognitive_complexity)]
pub fn preprocess<'c, Graph: LinkIterGraph + LinkIterable<NodeIdT>, ReorderBCC: Bool, Deg1: Bool, Deg2: Bool, Deg3: Bool>(graph: &Graph) -> Topocore {
    let order = dfs_pre_order(graph);

    let n = graph.num_nodes();

    let link_lexicographic = |a: &Link, b: &Link| match a.node.cmp(&b.node) {
        std::cmp::Ordering::Equal => a.weight.cmp(&b.weight),
        res => res,
    };

    let mut outs: Vec<Vec<Link>> = (0..n)
        .map(|rank| {
            let node = order.node(rank as NodeId);
            LinkIterable::<Link>::link_iter(graph, node)
                .filter(|&Link { node: head, .. }| head != node)
                .map(|Link { node, weight }| Link {
                    node: order.rank(node),
                    weight,
                })
                .collect::<Vec<_>>()
                .tap(|neighbors| neighbors.sort_unstable_by(link_lexicographic))
                .tap(|neighbors| neighbors.dedup_by(|a, b| a.node == b.node))
        })
        .collect();
    let reversed = OwnedGraph::reversed(graph);
    let mut ins: Vec<Vec<Link>> = (0..n)
        .map(|rank| {
            let node = order.node(rank as NodeId);
            LinkIterable::<Link>::link_iter(&reversed, node)
                .filter(|&Link { node: head, .. }| head != node)
                .map(|Link { node, weight }| Link {
                    node: order.rank(node),
                    weight,
                })
                .collect::<Vec<_>>()
                .tap(|neighbors| neighbors.sort_unstable_by(link_lexicographic))
                .tap(|neighbors| neighbors.dedup_by(|a, b| a.node == b.node))
        })
        .collect();

    let mut to_contract = BitVec::new(n);
    let mut queue = Vec::new();

    if ReorderBCC::VALUE {
        to_contract.set_all();

        let biggest = biconnected(&UndirectedGraph { ins: &reversed, outs: graph })
            .into_iter()
            .max_by_key(|edges| edges.len())
            .unwrap();
        for (u, v) in biggest {
            to_contract.unset(order.rank(u) as usize);
            to_contract.unset(order.rank(v) as usize);
        }
    }

    let deg_zero_or_one = |node_outs: &Vec<Link>, node_ins: &Vec<Link>| {
        node_outs.is_empty() || node_ins.is_empty() || (node_outs.len() == 1 && node_ins.len() == 1 && node_outs[0].node == node_ins[0].node)
    };

    if Deg1::VALUE {
        for node in 0..n {
            if !to_contract.get(node) {
                outs[node].retain(|link| !to_contract.get(link.node as usize));
                ins[node].retain(|link| !to_contract.get(link.node as usize));
            }
        }

        for node in 0..n {
            if !to_contract.get(node) {
                for &Link { node: neighbor, .. } in outs[node].iter().chain(&ins[node]) {
                    debug_assert!(!to_contract.get(neighbor as usize));
                }
            }
        }

        for node in 0..n {
            if deg_zero_or_one(&outs[node], &ins[node]) && !to_contract.get(node) {
                to_contract.set(node);
                queue.push(node);
            }
        }

        while let Some(node) = queue.pop() {
            for &Link { node: head, .. } in &outs[node] {
                let head_ins = &mut ins[head as usize];
                let pos = head_ins.iter().position(|&Link { node: tail, .. }| tail == node as NodeId).unwrap();
                head_ins.swap_remove(pos);

                if deg_zero_or_one(&outs[head as usize], head_ins) && !to_contract.get(head as usize) {
                    to_contract.set(head as usize);
                    queue.push(head as usize);
                }
            }

            for &Link { node: head, .. } in &ins[node] {
                let head_outs = &mut outs[head as usize];
                let pos = head_outs.iter().position(|&Link { node: tail, .. }| tail == node as NodeId).unwrap();
                head_outs.swap_remove(pos);

                if deg_zero_or_one(head_outs, &ins[head as usize]) && !to_contract.get(head as usize) {
                    to_contract.set(head as usize);
                    queue.push(head as usize);
                }
            }
        }

        for node in 0..n {
            if !to_contract.get(node) {
                debug_assert!(!deg_zero_or_one(&outs[node], &ins[node]));
                for &Link { node: neighbor, .. } in outs[node].iter().chain(&ins[node]) {
                    debug_assert!(!to_contract.get(neighbor as usize));
                }
            }
        }
    }

    let insert_or_decrease = |links: &mut Vec<Link>, target: NodeId, weight: Weight| {
        if let Some(link) = links.iter_mut().find(|&&mut Link { node, .. }| node == target) {
            link.weight = min(link.weight, weight);
        } else {
            links.push(Link { node: target, weight });
        }
    };

    if Deg2::VALUE {
        let deg_two = |node_outs: &Vec<Link>, node_ins: &Vec<Link>| match (&node_outs[..], &node_ins[..]) {
            (&[_], &[_]) => true,
            (&[Link { node: out_node, .. }], &[Link { node: in1, .. }, Link { node: in2, .. }]) => out_node == in1 || out_node == in2,
            (&[Link { node: out1, .. }, Link { node: out2, .. }], &[Link { node: in_node, .. }]) => in_node == out1 || in_node == out2,
            (&[Link { node: out1, .. }, Link { node: out2, .. }], &[Link { node: in1, .. }, Link { node: in2, .. }]) => {
                max(out1, out2) == max(in1, in2) && min(out1, out2) == min(in1, in2)
            }
            _ => false,
        };

        let deg_two_neighbors = |node_outs: &Vec<Link>, node_ins: &Vec<Link>| match (&node_outs[..], &node_ins[..]) {
            (&[Link { node: out_node, .. }], &[Link { node: in_node, .. }]) => (out_node, in_node),
            (&[Link { node: _out_node, .. }], &[Link { node: in1, .. }, Link { node: in2, .. }]) => (in1, in2),
            (&[Link { node: out1, .. }, Link { node: out2, .. }], &[Link { node: _in_node, .. }]) => (out1, out2),
            (&[Link { node: out1, .. }, Link { node: out2, .. }], &[Link { node: _in1, .. }, Link { node: _in2, .. }]) => (max(out1, out2), min(out1, out2)),
            _ => panic!("called neighbors on none deg 2 node"),
        };

        let deg_two_weights = |other: NodeId, node_outs: &Vec<Link>, node_ins: &Vec<Link>| {
            (
                node_outs
                    .iter()
                    .find(|&&Link { node: head, .. }| head == other)
                    .map(|&Link { weight, .. }| weight),
                node_ins
                    .iter()
                    .find(|&&Link { node: head, .. }| head == other)
                    .map(|&Link { weight, .. }| weight),
            )
        };

        for node in 0..n {
            if !to_contract.get(node) && deg_two(&outs[node], &ins[node]) {
                debug_assert!(!deg_zero_or_one(&outs[node], &ins[node]));
                to_contract.set(node);

                let mut prev = node;
                let mut next = outs[node][0].node as usize;
                while !deg_zero_or_one(&outs[next], &ins[next]) && deg_two(&outs[next as usize], &ins[next as usize]) && !to_contract.get(next) {
                    to_contract.set(next);
                    let (first, second) = deg_two_neighbors(&outs[next], &ins[next]);
                    if prev as NodeId == first {
                        prev = next as usize;
                        next = second as usize;
                    } else {
                        prev = next as usize;
                        next = first as usize;
                    }
                }

                debug_assert!(!deg_zero_or_one(&outs[next], &ins[next]));
                debug_assert!(!to_contract.get(next)); // isolated cycle

                let end1 = next;
                let end1_prev = prev;
                std::mem::swap(&mut next, &mut prev);

                let (mut forward, mut backward) = deg_two_weights(next as NodeId, &outs[prev], &ins[prev]);

                while !deg_zero_or_one(&outs[next], &ins[next]) && deg_two(&outs[next as usize], &ins[next as usize]) {
                    to_contract.set(next);
                    let (first, second) = deg_two_neighbors(&outs[next], &ins[next]);
                    if prev as NodeId == first {
                        prev = next as usize;
                        next = second as usize;
                    } else {
                        prev = next as usize;
                        next = first as usize;
                    }

                    let (next_forward, next_backward) = deg_two_weights(next as NodeId, &outs[prev], &ins[prev]);
                    forward = forward.and_then(|forward| next_forward.map(|next_forward| forward + next_forward));
                    backward = backward.and_then(|backward| next_backward.map(|next_backward| backward + next_backward));
                }
                let end2 = next;

                if let Some(pos) = outs[end1].iter().position(|&Link { node: head, .. }| head == end1_prev as NodeId) {
                    outs[end1].swap_remove(pos);
                }
                debug_assert_eq!(outs[end1].iter().position(|&Link { node: head, .. }| head == end1_prev as NodeId), None);
                if let Some(pos) = ins[end1].iter().position(|&Link { node: head, .. }| head == end1_prev as NodeId) {
                    ins[end1].swap_remove(pos);
                }
                debug_assert_eq!(ins[end1].iter().position(|&Link { node: head, .. }| head == end1_prev as NodeId), None);

                if let Some(pos) = outs[next].iter().position(|&Link { node: head, .. }| head == prev as NodeId) {
                    outs[next].swap_remove(pos);
                }
                debug_assert_eq!(outs[next].iter().position(|&Link { node: head, .. }| head == prev as NodeId), None);
                if let Some(pos) = ins[next].iter().position(|&Link { node: head, .. }| head == prev as NodeId) {
                    ins[next].swap_remove(pos);
                }
                debug_assert_eq!(ins[next].iter().position(|&Link { node: head, .. }| head == prev as NodeId), None);

                if let Some(weight) = forward {
                    if end1 != end2 {
                        insert_or_decrease(&mut outs[end1], end2 as NodeId, weight);
                        insert_or_decrease(&mut ins[end2], end1 as NodeId, weight);
                    }
                }
                if let Some(weight) = backward {
                    if end1 != end2 {
                        insert_or_decrease(&mut ins[end1], end2 as NodeId, weight);
                        insert_or_decrease(&mut outs[end2], end1 as NodeId, weight);
                    }
                }

                debug_assert!(!to_contract.get(end1));
                for &Link { node: neighbor, .. } in outs[end1].iter().chain(&ins[end1]) {
                    debug_assert!(
                        !to_contract.get(neighbor as usize),
                        "{} {} {} {} {} {}",
                        end1,
                        end1_prev,
                        neighbor,
                        end2,
                        prev,
                        node
                    );
                }
                debug_assert!(!to_contract.get(end2));
                for &Link { node: neighbor, .. } in outs[end2].iter().chain(&ins[end2]) {
                    debug_assert!(!to_contract.get(neighbor as usize));
                }
            }
        }

        for node in 0..n {
            if !to_contract.get(node) {
                for &Link { node: neighbor, .. } in outs[node].iter().chain(&ins[node]) {
                    debug_assert!(!to_contract.get(neighbor as usize));
                }
            }
        }
    }

    if Deg3::VALUE {
        let neighborhood = |node_outs: &Vec<Link>, node_ins: &Vec<Link>| {
            node_outs
                .iter()
                .chain(node_ins.iter())
                .map(|&Link { node, .. }| node)
                .collect::<Vec<_>>()
                .tap(|neighborhood| neighborhood.sort())
                .tap(|neighborhood| neighborhood.dedup())
        };

        for node in 0..n {
            if !to_contract.get(node) {
                let neighbors = neighborhood(&outs[node], &ins[node]);
                if neighbors.len() == 3 && neighbors.iter().all(|&neighbor| !to_contract.get(neighbor as usize)) {
                    to_contract.set(node);
                    queue.push(node);
                }
            }
        }

        while let Some(node) = queue.pop() {
            let mut node_out = Vec::new();
            let mut node_in = Vec::new();
            std::mem::swap(&mut node_out, &mut outs[node]);
            std::mem::swap(&mut node_in, &mut ins[node]);

            for &Link { node: head, .. } in &node_out {
                let pos = ins[head as usize].iter().position(|&Link { node: tail, .. }| tail == node as NodeId).unwrap();
                ins[head as usize].swap_remove(pos);
            }
            for &Link { node: head, .. } in &node_in {
                let pos = outs[head as usize].iter().position(|&Link { node: tail, .. }| tail == node as NodeId).unwrap();
                outs[head as usize].swap_remove(pos);
            }

            for &Link {
                node: head,
                weight: first_weight,
            } in &node_out
            {
                for &Link {
                    node: tail,
                    weight: second_weight,
                } in &node_in
                {
                    if head != tail {
                        insert_or_decrease(&mut outs[tail as usize], head, first_weight + second_weight);
                        insert_or_decrease(&mut ins[head as usize], tail, first_weight + second_weight);
                    }
                }
            }

            std::mem::swap(&mut node_out, &mut outs[node]);
            std::mem::swap(&mut node_in, &mut ins[node]);
        }

        for node in 0..n {
            if !to_contract.get(node) {
                for &Link { node: neighbor, .. } in outs[node].iter().chain(&ins[node]) {
                    debug_assert!(!to_contract.get(neighbor as usize));
                }
            }
        }
    }

    let mut new_order = Vec::with_capacity(n);

    let mut core_size = 0;

    for rank in 0..n {
        if !to_contract.get(rank) {
            new_order.push(order.node(rank as NodeId));
            core_size += 1;
        }
    }
    for rank in 0..n {
        if to_contract.get(rank) {
            new_order.push(order.node(rank as NodeId));
        }
    }

    let m_forward: usize = outs.iter().map(|links| links.len()).sum();
    let m_backward: usize = ins.iter().map(|links| links.len()).sum();

    let mut forward_first_out: Vec<EdgeId> = Vec::with_capacity(n + 1);
    forward_first_out.push(0);
    let mut backward_first_out: Vec<EdgeId> = Vec::with_capacity(n + 1);
    backward_first_out.push(0);
    let mut forward_head = Vec::with_capacity(m_forward);
    let mut forward_weight = Vec::with_capacity(m_forward);
    let mut backward_head = Vec::with_capacity(m_backward);
    let mut backward_weight = Vec::with_capacity(m_backward);

    let new_order = NodeOrder::from_node_order(new_order);
    for &orig_node in new_order.order() {
        let node = order.rank(orig_node);

        let node_outs = &outs[node as usize];
        forward_first_out.push(forward_first_out.last().unwrap() + node_outs.len() as EdgeId);
        for &Link { node: head, weight } in node_outs {
            forward_head.push(new_order.rank(order.node(head)));
            forward_weight.push(weight);
        }

        let node_ins = &ins[node as usize];
        backward_first_out.push(backward_first_out.last().unwrap() + node_ins.len() as EdgeId);
        for &Link { node: head, weight } in node_ins {
            backward_head.push(new_order.rank(order.node(head)));
            backward_weight.push(weight);
        }
    }

    Topocore {
        forward: OwnedGraph::new(forward_first_out, forward_head, forward_weight),
        backward: OwnedGraph::new(backward_first_out, backward_head, backward_weight),
        order: new_order,
        core_size,
    }
}

#[derive(Debug, Clone)]
pub struct VirtualTopocore {
    pub order: NodeOrder,
    biggest_bcc: usize,
    deg2: usize,
    deg3: usize,
    symmetric_degrees: std::sync::Arc<[u8]>,
    bcc: std::sync::Arc<[u32]>,
    bridge: std::sync::Arc<[InRangeOption<NodeId>]>,
}

#[derive(Debug, PartialEq)]
pub enum NodeType {
    OtherBCC(u8),
    Deg2OrLess,
    Deg3,
    Deg4OrMore,
}

impl NodeType {
    #[inline]
    pub fn in_core(&self) -> bool {
        if let NodeType::OtherBCC(_) = self {
            false
        } else {
            true
        }
    }
}

impl VirtualTopocore {
    #[inline]
    pub fn node_type(&self, node: NodeId) -> NodeType {
        let node = node as usize;
        if node < self.deg3 {
            NodeType::Deg4OrMore
        } else if node < self.deg2 {
            NodeType::Deg3
        } else if node < self.biggest_bcc {
            NodeType::Deg2OrLess
        } else {
            NodeType::OtherBCC(self.symmetric_degrees[node - self.biggest_bcc])
        }
    }

    pub fn bridge_node(&self, node: NodeId) -> Option<NodeId> {
        if self.node_type(node).in_core() {
            return None;
        }

        self.bridge[self.bcc[node as usize - self.biggest_bcc] as usize].value()
    }
}

#[allow(clippy::cognitive_complexity)]
pub fn virtual_topocore<'c, Graph: LinkIterable<NodeIdT>>(graph: &Graph) -> VirtualTopocore {
    let order = dfs_pre_order(graph);
    let n = graph.num_nodes();

    let reversed = UnweightedOwnedGraph::reversed(graph);

    let symmetric_degrees = (0..graph.num_nodes())
        .map(|node| {
            graph
                .link_iter(node as NodeId)
                .chain(LinkIterable::<NodeIdT>::link_iter(&reversed, node as NodeId))
                .collect::<Vec<NodeIdT>>()
                .tap(|neighbors| neighbors.sort_unstable())
                .tap(|neighbors| neighbors.dedup())
                .len() as u8
        })
        .collect::<Vec<u8>>();

    let biggest = biconnected(&UndirectedGraph { ins: &reversed, outs: graph })
        .into_iter()
        .max_by_key(|edges| edges.len())
        .unwrap();

    let mut biggest_bcc = BitVec::new(n);
    for (u, v) in biggest {
        biggest_bcc.set(u as usize);
        biggest_bcc.set(v as usize);
    }

    let mut components = vec![0; n];
    let mut bridge = Vec::new();
    let mut queue = std::collections::VecDeque::new();

    for node in 0..n {
        if !biggest_bcc.get(node) && components[node] == 0 {
            let component_idx = bridge.len() as u32 + 1;
            queue.push_back(node);
            components[node] = component_idx;
            while let Some(node) = queue.pop_front() {
                for NodeIdT(neighbor) in (UndirectedGraph { ins: &reversed, outs: graph }).link_iter(node as NodeId) {
                    if biggest_bcc.get(neighbor as usize) {
                        if bridge.len() < component_idx as usize {
                            bridge.push(InRangeOption::new(Some(neighbor)));
                        } else {
                            debug_assert_eq!(bridge.last().unwrap().value().unwrap(), neighbor);
                        }
                    } else {
                        if components[neighbor as usize] == 0 {
                            components[neighbor as usize] = component_idx;
                            queue.push_back(neighbor as usize);
                        } else {
                            debug_assert_eq!(components[neighbor as usize], component_idx);
                        }
                    }
                }
            }

            if bridge.len() < component_idx as usize {
                bridge.push(InRangeOption::new(None));
            }
        }
    }

    let mut new_order = Vec::with_capacity(n);

    let mut biggest_bcc_size = 0;

    for &node in order.order() {
        if biggest_bcc.get(node as usize) {
            new_order.push(node);
            biggest_bcc_size += 1;
        }
    }
    new_order.sort_by(|&n1, &n2| {
        if symmetric_degrees[n1 as usize] > 3 && symmetric_degrees[n2 as usize] > 3 {
            std::cmp::Ordering::Equal
        } else {
            symmetric_degrees[n2 as usize].cmp(&symmetric_degrees[n1 as usize])
        }
    });
    let mut other_bccs_symmetric_degrees = Vec::with_capacity(n - biggest_bcc_size);
    let mut bcc = Vec::with_capacity(n - biggest_bcc_size);
    for &node in order.order() {
        if !biggest_bcc.get(node as usize) {
            new_order.push(node);
            other_bccs_symmetric_degrees.push(symmetric_degrees[node as usize]);
            bcc.push(components[node as usize] - 1);
        }
    }
    let deg3 = new_order.iter().position(|&node| symmetric_degrees[node as usize] < 4).unwrap_or(n as usize);
    let deg2 = new_order.iter().position(|&node| symmetric_degrees[node as usize] < 3).unwrap_or(n as usize);

    report!("biggest_bcc_size", biggest_bcc_size);
    report!("deg2", deg2);
    report!("deg3", deg3);

    let order = NodeOrder::from_node_order(new_order);

    for bridge_node in &mut bridge {
        if let Some(bridge) = bridge_node.value() {
            *bridge_node = InRangeOption::new(Some(order.rank(bridge)));
        }
    }

    VirtualTopocore {
        order,
        biggest_bcc: biggest_bcc_size,
        deg3,
        deg2,
        symmetric_degrees: other_bccs_symmetric_degrees.into(),
        bcc: bcc.into(),
        bridge: bridge.into(),
    }
}

#[derive(Clone)]
pub struct VirtualTopocoreGraph<G> {
    pub graph: G,
    virtual_topocore: VirtualTopocore,
}

impl<Graph> VirtualTopocoreGraph<Graph> {
    pub fn new<G>(graph: &G) -> (Self, Self, VirtualTopocore)
    where
        G: LinkIterable<NodeIdT>,
        Graph: BuildPermutated<G>,
    {
        let topocore = virtual_topocore(graph);
        (
            VirtualTopocoreGraph {
                graph: <Graph as BuildPermutated<G>>::permutated(&graph, &topocore.order),
                virtual_topocore: topocore.clone(),
            },
            VirtualTopocoreGraph {
                graph: <Graph as BuildPermutated<G>>::permutated_filtered(&graph, &topocore.order, Box::new(|_, _| false)),
                virtual_topocore: topocore.clone(),
            },
            topocore,
        )
    }

    pub fn new_topo_dijkstra_graphs<G>(graph: &G) -> (Self, Self, VirtualTopocore)
    where
        G: LinkIterable<NodeIdT>,
        Graph: BuildPermutated<G>,
    {
        let topocore = virtual_topocore(graph);
        (
            VirtualTopocoreGraph {
                graph: <Graph as BuildPermutated<G>>::permutated_filtered(&graph, &topocore.order, {
                    let topocore = topocore.clone();
                    Box::new(move |t, h| topocore.node_type(t).in_core() && topocore.node_type(h).in_core())
                }),
                virtual_topocore: topocore.clone(),
            },
            VirtualTopocoreGraph {
                graph: <Graph as BuildPermutated<G>>::permutated_filtered(&graph, &topocore.order, {
                    let topocore = topocore.clone();
                    Box::new(move |t, h| !topocore.node_type(t).in_core() || !topocore.node_type(h).in_core())
                }),
                virtual_topocore: topocore.clone(),
            },
            topocore,
        )
    }
}

impl<G: Graph> Graph for VirtualTopocoreGraph<G> {
    fn degree(&self, node: NodeId) -> usize {
        self.graph.degree(node)
    }
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
}

impl<G: LinkIterable<L>, L> LinkIterable<L> for VirtualTopocoreGraph<G> {
    type Iter<'a> = <G as LinkIterable<L>>::Iter<'a>;

    #[inline(always)]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.graph.link_iter(node)
    }
}

impl<G> SymmetricDegreeGraph for VirtualTopocoreGraph<G> {
    #[inline(always)]
    fn symmetric_degree(&self, node: NodeId) -> SymmetricDeg {
        let node = node as usize;
        if node < self.virtual_topocore.deg3 {
            SymmetricDeg::GreaterEqFour
        } else if node < self.virtual_topocore.deg2 {
            SymmetricDeg::Three
        } else if node < self.virtual_topocore.biggest_bcc {
            SymmetricDeg::LessEqTwo
        } else {
            match self.virtual_topocore.symmetric_degrees[node - self.virtual_topocore.biggest_bcc].cmp(&3) {
                std::cmp::Ordering::Equal => SymmetricDeg::Three,
                std::cmp::Ordering::Greater => SymmetricDeg::GreaterEqFour,
                std::cmp::Ordering::Less => SymmetricDeg::LessEqTwo,
            }
        }
    }
}

impl<G: BuildReversed<G>> BuildReversed<VirtualTopocoreGraph<G>> for VirtualTopocoreGraph<G> {
    fn reversed(graph: &VirtualTopocoreGraph<G>) -> Self {
        Self {
            graph: G::reversed(&graph.graph),
            virtual_topocore: graph.virtual_topocore.clone(),
        }
    }
}

impl<G: EdgeIdGraph> EdgeIdGraph for VirtualTopocoreGraph<G> {
    // https://github.com/rust-lang/rustfmt/issues/4911
    #[rustfmt::skip]
    type IdxIter<'a> where Self: 'a = G::IdxIter<'a>;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.graph.edge_indices(from, to)
    }
    fn neighbor_edge_indices(&self, node: NodeId) -> std::ops::Range<EdgeId> {
        self.graph.neighbor_edge_indices(node)
    }
}

impl<E, G: EdgeRandomAccessGraph<E>> EdgeRandomAccessGraph<E> for VirtualTopocoreGraph<G> {
    fn link(&self, edge_id: EdgeId) -> E {
        self.graph.link(edge_id)
    }
}

fn dfs_pre_order<Graph: LinkIterable<NodeIdT>>(graph: &Graph) -> NodeOrder {
    let mut order = Vec::with_capacity(graph.num_nodes());
    dfs(graph, &mut |node| {
        order.push(node);
    });
    NodeOrder::from_node_order(order)
}

fn dfs<Graph: LinkIterable<NodeIdT>>(graph: &Graph, visit: &mut impl FnMut(NodeId)) {
    let mut visited = BitVec::new(graph.num_nodes());
    let mut stack = Vec::new();
    for node in 0..graph.num_nodes() {
        if !visited.get(node) {
            stack.push(node as NodeId);
        }

        while let Some(node) = stack.pop() {
            if visited.get(node as usize) {
                continue;
            }
            visit(node);
            visited.set(node as usize);

            for NodeIdT(head) in graph.link_iter(node) {
                if !visited.get(head as usize) {
                    stack.push(head);
                }
            }
        }
    }
}

fn biconnected<Graph: LinkIterable<NodeIdT>>(graph: &Graph) -> Vec<Vec<(NodeId, NodeId)>> {
    let mut stack = Vec::new();

    let mut dfs_num_counter = 0;
    let mut dfs_num = vec![InRangeOption::<usize>::new(None); graph.num_nodes()];
    let mut dfs_low = vec![0; graph.num_nodes()];
    let mut dfs_parent = vec![0; graph.num_nodes()];
    let mut edge_stack = Vec::new();
    let mut components = Vec::new();

    for node in 0..graph.num_nodes() {
        if dfs_num[node].value().is_some() {
            continue;
        }

        debug_assert!(edge_stack.is_empty());
        debug_assert!(stack.is_empty());
        dfs_parent[node] = node;
        stack.push((node, graph.link_iter(node as NodeId)));

        while let Some(&mut (node, ref mut neighbors)) = stack.last_mut() {
            if dfs_num[node].value().is_none() {
                dfs_low[node] = dfs_num_counter;
                dfs_num[node] = InRangeOption::new(Some(dfs_num_counter));
                dfs_num_counter += 1;
            }

            if let Some(NodeIdT(neighbor)) = neighbors.next() {
                if let Some(neighbor_dfs_num) = dfs_num[neighbor as usize].value() {
                    if neighbor_dfs_num < dfs_num[node].value().unwrap() && (neighbor as usize) != dfs_parent[node] {
                        edge_stack.push((node as NodeId, neighbor));
                        dfs_low[node] = std::cmp::min(dfs_low[node], neighbor_dfs_num);
                    }
                } else {
                    dfs_parent[neighbor as usize] = node;
                    edge_stack.push((node as NodeId, neighbor));
                    stack.push((neighbor as usize, graph.link_iter(neighbor)));
                }
            } else {
                stack.pop();
                let v = dfs_parent[node];
                let w = node;
                dfs_low[v] = std::cmp::min(dfs_low[v], dfs_low[w]);

                if dfs_low[w] >= dfs_num[v].value().unwrap() {
                    let mut component = Vec::new();
                    while let Some(&(u_1, _u_2)) = edge_stack.last() {
                        if dfs_num[u_1 as usize].value().unwrap() >= dfs_num[w].value().unwrap() {
                            component.push(edge_stack.pop().unwrap());
                        } else {
                            break;
                        }
                    }
                    if let Some((u_1, u_2)) = edge_stack.pop() {
                        debug_assert_eq!((u_1, u_2), (v as NodeId, w as NodeId));
                        component.push((u_1, u_2));
                    }
                    components.push(component);
                }
            }
        }
    }

    components
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn test_minimal_chain() {
    //     let first_out = vec![0,  1,     3,  4];
    //     let head =      vec![1,  0, 2,  1];
    //     let weight =    vec![1,  1, 1,  1];
    //     let graph = OwnedGraph::new(first_out, head, weight);
    //     let mut topocore = preprocess(&graph);
    //     assert_eq!(topocore.distance(0, 2), Some(2));
    //     assert_eq!(topocore.distance(2, 0), Some(2));
    // }

    // #[test]
    // fn test_triangle() {
    //     let first_out = vec![0,  1,  2,  3];
    //     let head =      vec![1,  2,  0];
    //     let weight =    vec![1,  1,  1];
    //     let graph = OwnedGraph::new(first_out, head, weight);
    //     let mut topocore = preprocess(&graph);
    //     assert_eq!(topocore.distance(0, 2), Some(2));
    //     assert_eq!(topocore.distance(0, 1), Some(1));
    //     assert_eq!(topocore.distance(2, 0), Some(1));
    //     assert_eq!(topocore.distance(2, 1), Some(2));
    // }

    // #[test]
    // fn test_square_with_diag() {
    //     let first_out = vec![0,     3,   5,     8,  10];
    //     let head =      vec![1,2,3, 0,2, 0,1,3, 0,2];
    //     let weight =    vec![1,5,2, 1,1, 5,1,2, 2,2];
    //     let graph = OwnedGraph::new(first_out, head, weight);
    //     let mut topocore = preprocess(&graph);
    //     assert_eq!(topocore.distance(0, 1), Some(1));
    //     assert_eq!(topocore.distance(1, 0), Some(1));
    //     assert_eq!(topocore.distance(0, 2), Some(2));
    //     assert_eq!(topocore.distance(2, 0), Some(2));
    //     assert_eq!(topocore.distance(0, 3), Some(2));
    //     assert_eq!(topocore.distance(3, 0), Some(2));
    //     assert_eq!(topocore.distance(1, 2), Some(1));
    //     assert_eq!(topocore.distance(2, 1), Some(1));
    //     assert_eq!(topocore.distance(1, 3), Some(3));
    //     assert_eq!(topocore.distance(3, 1), Some(3));
    //     assert_eq!(topocore.distance(2, 3), Some(2));
    //     assert_eq!(topocore.distance(3, 2), Some(2));
    // }
}
