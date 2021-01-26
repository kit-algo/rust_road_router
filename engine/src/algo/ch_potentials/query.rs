use super::*;

use crate::datastr::rank_select_map::FastClearBitVec;
use crate::{
    algo::{dijkstra::gen_topo_dijkstra::*, topocore::*},
    datastr::graph::time_dependent::*,
};
#[cfg(feature = "chpot-visualize")]
use std::rc::Rc;

pub struct Server<P, Ops: DijkstraOps<Graph>, Graph> {
    forward_dijkstra: GenTopoDijkstra<VirtualTopocoreOps<Ops>, VirtualTopocoreGraph<Graph>>,
    #[cfg(not(feature = "chpot-no-bcc"))]
    into_comp_graph: VirtualTopocoreGraph<Graph>,
    #[cfg(not(feature = "chpot-no-bcc"))]
    reversed_into_comp_graph: UnweightedOwnedGraph,

    potential: P,

    reversed: UnweightedOwnedGraph,
    virtual_topocore: VirtualTopocore,
    visited: FastClearBitVec,

    #[cfg(feature = "chpot-visualize")]
    lat: Rc<[f32]>,
    #[cfg(feature = "chpot-visualize")]
    lng: Rc<[f32]>,
}

impl<P: Potential, Ops: DijkstraOps<Graph, Label = Timestamp>, Graph> Server<P, Ops, Graph>
where
    Graph: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Ops::Arc>,
{
    pub fn new<G>(
        graph: &G,
        potential: P,
        ops: Ops,
        #[cfg(feature = "chpot-visualize")] lat: Rc<[f32]>,
        #[cfg(feature = "chpot-visualize")] lng: Rc<[f32]>,
    ) -> Self
    where
        G: for<'a> LinkIterable<'a, NodeId>,
        Graph: BuildPermutated<G>,
    {
        report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro", move || {
            let n = graph.num_nodes();
            #[cfg(feature = "chpot-no-bcc")]
            {
                let (graph, virtual_topocore) = VirtualTopocoreGraph::new(graph);
                let reversed = UnweightedOwnedGraph::reversed(&graph);
                Self {
                    forward_dijkstra: GenTopoDijkstra::new_with_ops(graph, VirtualTopocoreOps(ops)),
                    potential,

                    reversed,
                    virtual_topocore,
                    visited: FastClearBitVec::new(n),

                    #[cfg(feature = "chpot-visualize")]
                    lat,
                    #[cfg(feature = "chpot-visualize")]
                    lng,
                }
            }
            #[cfg(not(feature = "chpot-no-bcc"))]
            {
                let (main_graph, into_comp_graph, virtual_topocore) = VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph);
                let reversed = UnweightedOwnedGraph::reversed(&main_graph);
                let reversed_into_comp_graph = UnweightedOwnedGraph::reversed(&into_comp_graph);

                Self {
                    forward_dijkstra: GenTopoDijkstra::new_with_ops(main_graph, VirtualTopocoreOps(ops)),
                    into_comp_graph,
                    reversed_into_comp_graph,
                    potential,

                    reversed,
                    virtual_topocore,
                    visited: FastClearBitVec::new(n),

                    #[cfg(feature = "chpot-visualize")]
                    lat,
                    #[cfg(feature = "chpot-visualize")]
                    lng,
                }
            }
        })
    }

    fn dfs(
        graph: &UnweightedOwnedGraph,
        node: NodeId,
        visited: &mut FastClearBitVec,
        border_callback: &mut impl FnMut(NodeId),
        in_core: &mut impl FnMut(NodeId) -> bool,
    ) {
        if visited.get(node as usize) {
            return;
        }
        visited.set(node as usize);
        if in_core(node) {
            border_callback(node);
            return;
        }
        for head in graph.link_iter(node) {
            Self::dfs(graph, head, visited, border_callback, in_core);
        }
    }

    #[cfg(not(feature = "chpot-no-bcc"))]
    fn border(&mut self, node: NodeId) -> Option<NodeId> {
        let mut border = None;
        self.visited.clear();
        let virtual_topocore = &self.virtual_topocore;
        Self::dfs(
            &self.reversed_into_comp_graph,
            node,
            &mut self.visited,
            &mut |node| {
                let prev = border.replace(node);
                debug_assert_eq!(prev, None);
            },
            &mut |node| virtual_topocore.node_type(node).in_core(),
        );
        border
    }

    fn distance(&mut self, mut query: impl GenQuery<Timestamp> + Copy) -> Option<Weight> {
        let to = query.to();
        let _from = query.from();
        query.permutate(&self.virtual_topocore.order);

        report!("algo", "CH Potentials Query");

        let departure = query.initial_state();

        #[cfg(feature = "chpot-visualize")]
        {
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[_from as usize], self.lng[_from as usize]
            );
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[to as usize], self.lng[to as usize]
            );
        };
        let mut num_queue_pops = 0;

        self.forward_dijkstra.initialize_query(query);
        self.potential.init(to);
        #[cfg(not(feature = "chpot-no-bcc"))]
        let border = self.border(query.to());
        let forward_dijkstra = &mut self.forward_dijkstra;
        let virtual_topocore = &self.virtual_topocore;
        let potential = &mut self.potential;

        if cfg!(feature = "chpot-no-bcc") || self.virtual_topocore.node_type(query.to()).in_core() {
            let mut counter = 0;
            self.visited.clear();
            Self::dfs(&self.reversed, query.to(), &mut self.visited, &mut |_| {}, &mut |_| {
                if counter < 100 {
                    counter += 1;
                    false
                } else {
                    true
                }
            });

            if counter < 100 {
                return None;
            }
        }

        #[cfg(not(feature = "chpot-no-bcc"))]
        {
            let border_node = if let Some(border_node) = border { border_node } else { return None };
            let border_node_pot = if let Some(pot) = potential.potential(self.virtual_topocore.order.node(border_node)) {
                pot
            } else {
                return None;
            };

            while let Some(node) = forward_dijkstra.next_step_with_potential(|node| potential.potential(virtual_topocore.order.node(node))) {
                num_queue_pops += 1;
                #[cfg(feature = "chpot-visualize")]
                {
                    let node_id = virtual_topocore.order.node(node) as usize;
                    println!(
                        "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                        self.lat[node_id], self.lng[node_id], num_queue_pops
                    );
                    println!(
                        "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                        node_id,
                        forward_dijkstra.tentative_distance(node),
                        potential.potential(node_id as NodeId).unwrap()
                    );
                };

                if node == query.to()
                    || forward_dijkstra
                        .queue()
                        .peek()
                        .map(|e| e.key >= *forward_dijkstra.tentative_distance(border_node) + border_node_pot)
                        .unwrap_or(false)
                {
                    break;
                }
            }

            forward_dijkstra.swap_graph(&mut self.into_comp_graph);
            forward_dijkstra.reinit_queue(border_node);
        }

        while let Some(node) = forward_dijkstra.next_step_with_potential(|node| potential.potential(virtual_topocore.order.node(node))) {
            num_queue_pops += 1;
            #[cfg(feature = "chpot-visualize")]
            {
                let node_id = virtual_topocore.order.node(node) as usize;
                println!(
                    "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                    self.lat[node_id], self.lng[node_id], num_queue_pops
                );
                println!(
                    "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                    node_id,
                    forward_dijkstra.tentative_distance(node),
                    potential.potential(node_id as NodeId).unwrap()
                );
            };

            if node == query.to()
                || forward_dijkstra
                    .queue()
                    .peek()
                    .map(|e| e.key >= *forward_dijkstra.tentative_distance(query.to()))
                    .unwrap_or(false)
            {
                break;
            }
        }

        #[cfg(not(feature = "chpot-no-bcc"))]
        forward_dijkstra.swap_graph(&mut self.into_comp_graph);

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", forward_dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs());

        let dist = *forward_dijkstra.tentative_distance(query.to());
        if dist < INFINITY {
            Some(dist - departure)
        } else {
            None
        }
    }

    fn path(&self, mut query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        query.permutate(&self.virtual_topocore.order);
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, P, O: DijkstraOps<G>, G, Q>(&'s mut Server<P, O, G>, Q);

impl<'s, P, O, G, Q> PathServer for PathServerWrapper<'s, P, O, G, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, P, O, G, Q> PathServerWrapper<'s, P, O, G, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blackIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = *self.0.forward_dijkstra.tentative_distance(node);
            let pot = self.lower_bound(node).unwrap_or(INFINITY);
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>lower_bound: {}<br>sum: {}\");",
                node,
                dist / 1000,
                pot / 1000,
                (pot + dist) / 1000
            );
        }
    }

    pub fn potential(&self) -> &P {
        &self.0.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.potential.potential(node)
    }
}

impl<'s, P: 's, O: 's, G: 's> TDQueryServer<'s, Timestamp, Weight> for Server<P, O, G>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, P, O, G, TDQuery<Timestamp>>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, P: 's, O: 's, G: 's> QueryServer<'s> for Server<P, O, G>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, P, O, G, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

struct VirtualTopocoreOps<O>(O);

impl<G, O> DijkstraOps<VirtualTopocoreGraph<G>> for VirtualTopocoreOps<O>
where
    O: DijkstraOps<G>,
{
    type Label = O::Label;
    type Arc = O::Arc;
    type LinkResult = O::LinkResult;

    #[inline(always)]
    fn link(&mut self, graph: &VirtualTopocoreGraph<G>, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult {
        self.0.link(&graph.graph, label, link)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool {
        self.0.merge(label, linked)
    }
}
