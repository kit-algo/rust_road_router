use super::*;
use crate::algo::customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *};
use crate::algo::dijkstra::topo_dijkstra::TopoDijkstra;
use crate::datastr::node_order::NodeOrder;
use crate::datastr::rank_select_map::*;
use crate::datastr::timestamped_vector::TimestampedVector;
use crate::util::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct Server<'a> {
    forward_dijkstra: TopoDijkstra<OwnedGraph>,
    backward_graph: OwnedGraph,
    order: NodeOrder,
    core_size: usize,
    meeting_node: NodeId,
    stack: Vec<NodeId>,

    cch: &'a CCH,

    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>,
    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    border_nodes: FastClearBitVec,

    visited: FastClearBitVec,

    #[cfg(feature = "chpot_visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot_visualize")]
    lng: &[f32],
}

impl<'a> Server<'a> {
    pub fn new<Graph>(
        topocore: crate::algo::topocore::Topocore,
        cch: &'a CCH,
        lower_bound: &Graph,
        #[cfg(feature = "chpot_visualize")] lat: &[f32],
        #[cfg(feature = "chpot_visualize")] lng: &[f32],
    ) -> Server<'a>
    where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph + Sync,
    {
        let customized = customize(cch, lower_bound);
        let (forward_up_graph, backward_up_graph) = customized.into_ch_graphs();
        let backward_elimination_tree = SteppedEliminationTree::new(backward_up_graph, cch.elimination_tree());

        Server {
            forward_dijkstra: TopoDijkstra::new(topocore.forward),
            backward_graph: topocore.backward,
            core_size: topocore.core_size,
            meeting_node: 0,
            stack: Vec::new(),
            order: topocore.order,
            cch,
            forward_cch_graph: forward_up_graph,
            backward_elimination_tree,
            potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
            border_nodes: FastClearBitVec::new(topocore.core_size),
            visited: FastClearBitVec::new(cch.num_nodes()),

            #[cfg(feature = "chpot_visualize")]
            lat,
            #[cfg(feature = "chpot_visualize")]
            lng,
        }
    }

    fn dfs(graph: &OwnedGraph, node: NodeId, visited: &mut FastClearBitVec, border_callback: &mut impl FnMut(NodeId), in_core: &impl Fn(NodeId) -> bool) {
        if visited.get(node as usize) {
            return;
        }
        visited.set(node as usize);
        if in_core(node) {
            border_callback(node);
            return;
        }
        for link in graph.neighbor_iter(node) {
            Self::dfs(graph, link.node, visited, border_callback, in_core);
        }
    }

    fn border(&mut self, node: NodeId, in_core: &impl Fn(NodeId) -> bool) -> Vec<NodeId> {
        let mut border = Vec::new();
        self.visited.clear();
        Self::dfs(&self.backward_graph, node, &mut self.visited, &mut |node| border.push(node), in_core);
        border
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        #[cfg(feature = "chpot_visualize")]
        {
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[from as usize], self.lng[from as usize]
            );
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[to as usize], self.lng[to as usize]
            );
        };

        self.potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(to));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }

        let from = self.order.rank(from);
        let to = self.order.rank(to);

        // node ids were reordered so that core nodes are at the "front" of the order, that is come first
        // so we can check if a node is in core by checking if its id is smaller than the number of nodes in the core
        let in_core = {
            let core_size = self.core_size as NodeId;
            move |node| node < core_size
        };

        self.border_nodes.clear();
        let border = self.border(to, &in_core);
        for border_node in border {
            self.border_nodes.set(border_node as usize);
        }

        self.forward_dijkstra.initialize_query(Query { from, to });

        let forward_dijkstra = &mut self.forward_dijkstra;
        let stack = &mut self.stack;

        let cch = &self.cch;
        let order = &self.order;

        let potentials = &mut self.potentials;
        let backward_elimination_tree = &self.backward_elimination_tree;
        let forward_cch_graph = &self.forward_cch_graph;

        let border_nodes = &self.border_nodes;

        loop {
            match forward_dijkstra.next_step_with_potential(
                |node, distance| {
                    debug_assert!(distance < INFINITY);
                    Self::potential(
                        potentials,
                        forward_cch_graph,
                        backward_elimination_tree,
                        cch.node_order().rank(order.node(node)),
                        stack,
                    )
                    .map(|pot| distance + pot)
                },
                |tail, head| in_core(head) || !in_core(tail) || border_nodes.get(tail as usize),
            ) {
                QueryProgress::Settled(State { node: _node, .. }) => {
                    #[cfg(feature = "chpot_visualize")]
                    {
                        let node_id = self.order.node(_node) as usize;
                        println!(
                            "var marker = L.marker([{}, {}], {{ icon: blueIcon }}).addTo(map);",
                            self.lat[node_id], self.lng[node_id]
                        );
                        println!(
                            "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                            node_id,
                            distance,
                            Self::potential(
                                potentials,
                                forward_elimination_tree,
                                backward_elimination_tree,
                                cch.node_order().rank(order.node(_node)),
                                stack
                            )
                        );
                    };
                }
                QueryProgress::Done(result) => return result,
            }
        }
    }

    fn potential(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        forward: &FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
        backward: &SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        node: NodeId,
        stack: &mut Vec<NodeId>,
    ) -> Option<Weight> {
        let mut cur_node = node;
        while potentials[cur_node as usize].value().is_none() {
            stack.push(cur_node);
            if let Some(parent) = backward.parent(cur_node).value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = stack.pop() {
            let min_by_up = forward
                .neighbor_iter(node)
                .map(|edge| edge.weight + potentials[edge.node as usize].value().unwrap())
                .min()
                .unwrap_or(INFINITY);

            potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(backward.tentative_distance(node), min_by_up)));
        }

        let dist = potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }

    fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.forward_dijkstra.query().to);

        while *path.last().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        for node in &mut path {
            *node = self.order.node(*node);
        }

        path
    }
}

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>);

impl<'s, 'a> PathServer for PathServerWrapper<'s, 'a> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, 'a: 's> QueryServer<'s> for Server<'a> {
    type P = PathServerWrapper<'s, 'a>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
