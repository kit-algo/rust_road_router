use super::*;

use crate::datastr::rank_select_map::FastClearBitVec;
use crate::{
    algo::{a_star::*, dijkstra::gen_topo_dijkstra::*, topocore::*},
    datastr::graph::time_dependent::*,
};

pub struct Server<Graph = OwnedGraph, Ops = DefaultOps, P = ZeroPotential>
where
    Ops: DijkstraOps<Graph>,
{
    core_search: SkipLowDegServer<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>, PotentialForPermutated<P>>,
    #[cfg(not(feature = "chpot-no-bcc"))]
    comp_search: GenTopoDijkstra<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>>,
    #[cfg(not(feature = "chpot-no-bcc"))]
    reversed_comp_graph: UnweightedOwnedGraph,
    #[cfg(not(feature = "chpot-no-bcc"))]
    comp_to_core: NodeId,

    virtual_topocore: VirtualTopocore,
    #[cfg(not(feature = "chpot-no-bcc"))]
    visited: FastClearBitVec,
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential> Server<Graph, Ops, P>
where
    Graph: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Ops::Arc>,
{
    pub fn new<G>(graph: &G, potential: P, ops: Ops) -> Self
    where
        G: for<'a> LinkIterable<'a, NodeId>,
        Graph: BuildPermutated<G>,
    {
        report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro", move || {
            let n = graph.num_nodes();
            #[cfg(not(feature = "chpot-no-bcc"))]
            {
                let (main_graph, comp_graph, virtual_topocore) = VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph);
                let reversed_comp_graph = UnweightedOwnedGraph::reversed(&comp_graph);

                Self {
                    core_search: SkipLowDegServer::new(
                        main_graph,
                        PotentialForPermutated {
                            order: virtual_topocore.order.clone(),
                            potential,
                        },
                        VirtualTopocoreOps(ops),
                    ),
                    comp_search: GenTopoDijkstra::new_with_ops(comp_graph, VirtualTopocoreOps(ops)),
                    reversed_comp_graph,
                    comp_to_core: n as NodeId,

                    virtual_topocore,
                    visited: FastClearBitVec::new(n),
                }
            }
            #[cfg(feature = "chpot-no-bcc")]
            {
                let (main_graph, virtual_topocore) = VirtualTopocoreGraph::new(graph);

                Self {
                    core_search: SkipLowDegServer::new(
                        main_graph,
                        PotentialForPermutated {
                            order: virtual_topocore.order.clone(),
                            potential,
                        },
                        VirtualTopocoreOps(ops),
                    ),

                    virtual_topocore,
                }
            }
        })
    }

    #[cfg(not(feature = "chpot-no-bcc"))]
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
            &self.reversed_comp_graph,
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

    #[cfg(feature = "chpot-no-bcc")]
    fn distance<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        mut query: Q,
        mut inspect: impl FnMut(NodeId, &NodeOrder, &GenTopoDijkstra<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>>, &mut PotentialForPermutated<P>),
    ) -> Option<Weight> {
        query.permutate(&self.virtual_topocore.order);
        let order = &self.virtual_topocore.order;
        self.core_search.distance(query, |n, d, p| inspect(n, order, d, p))
    }

    #[cfg(not(feature = "chpot-no-bcc"))]
    fn distance<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        mut query: Q,
        mut inspect: impl FnMut(NodeId, &NodeOrder, &GenTopoDijkstra<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>>, &mut PotentialForPermutated<P>),
    ) -> Option<Weight> {
        query.permutate(&self.virtual_topocore.order);

        report!("algo", "Virtual Topocore Component Query");

        let departure = query.initial_state();

        let mut num_queue_pops = 0;

        self.comp_search.initialize_query(query);
        self.core_search.potential.init(query.to());
        let border = self.border(query.to());

        let comp_search = &mut self.comp_search;
        let virtual_topocore = &self.virtual_topocore;
        let potential = &mut self.core_search.potential;

        let border_node = if let Some(border_node) = border { border_node } else { return None };

        while let Some(node) = comp_search.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, &virtual_topocore.order, comp_search, potential);
            if node == query.to() {
                return Some(comp_search.tentative_distance(query.to()) - departure);
            }
            if virtual_topocore.node_type(node).in_core() {
                self.comp_to_core = node;
                break;
            }
        }

        {
            let _ctxt = push_context("core_search".to_string());
            let core_query = Q::new(self.comp_to_core, border_node, *comp_search.tentative_distance(self.comp_to_core));
            self.core_search.forward_dijkstra.initialize_query(core_query);
            self.core_search
                .forward_dijkstra
                .overwrite_distance(self.comp_to_core, *comp_search.tentative_distance(self.comp_to_core));
            self.core_search.forward_dijkstra.reinit_queue(self.comp_to_core);
            self.core_search
                .distance_manually_initialized(core_query, |n, d, p| inspect(n, &virtual_topocore.order, d, p));
        }

        comp_search.overwrite_distance(border_node, *self.core_search.forward_dijkstra.tentative_distance(border_node));
        comp_search.reinit_queue(border_node);

        let potential = &mut self.core_search.potential;

        while let Some(node) = comp_search.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, &virtual_topocore.order, comp_search, potential);

            if comp_search
                .queue()
                .peek()
                .map(|e| e.key >= *comp_search.tentative_distance(query.to()))
                .unwrap_or(true)
            {
                break;
            }
        }

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", comp_search.num_queue_pushs());
        report!("num_relaxed_arcs", comp_search.num_relaxed_arcs());

        let dist = *comp_search.tentative_distance(query.to());
        if dist < INFINITY {
            Some(dist - departure)
        } else {
            None
        }
    }

    pub fn visualize_query(&mut self, query: impl GenQuery<Timestamp> + Copy, lat: &[f32], lng: &[f32]) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance(query, |node, order, dijk, pot| {
            let node_id = order.node(node) as usize;
            println!(
                "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                lat[node_id], lng[node_id], num_settled_nodes
            );
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                node_id,
                dijk.tentative_distance(node),
                pot.potential(node_id as NodeId).unwrap()
            );
            num_settled_nodes += 1;
        });
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.from() as usize],
            lng[query.from() as usize]
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.to() as usize],
            lng[query.to() as usize]
        );
        res
    }

    fn path(&self, mut query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        query.permutate(&self.virtual_topocore.order);
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();
        for node in &mut path {
            *node = self.virtual_topocore.order.node(*node);
        }

        path
    }

    #[cfg(not(feature = "chpot-no-bcc"))]
    fn tentative_distance(&self, node: NodeId) -> Weight {
        let rank = self.virtual_topocore.order.rank(node);
        if self.virtual_topocore.node_type(rank).in_core() {
            *self.core_search.forward_dijkstra.tentative_distance(rank)
        } else {
            *self.comp_search.tentative_distance(rank)
        }
    }

    #[cfg(feature = "chpot-no-bcc")]
    fn tentative_distance(&self, node: NodeId) -> Weight {
        let rank = self.virtual_topocore.order.rank(node);
        *self.core_search.forward_dijkstra.tentative_distance(rank)
    }

    #[cfg(not(feature = "chpot-no-bcc"))]
    fn predecessor(&self, node: NodeId) -> NodeId {
        let rank = self.virtual_topocore.order.rank(node);
        if self.virtual_topocore.node_type(rank).in_core() && rank != self.comp_to_core {
            self.core_search.forward_dijkstra.predecessor(rank)
        } else {
            self.comp_search.predecessor(rank)
        }
    }

    #[cfg(feature = "chpot-no-bcc")]
    fn predecessor(&self, node: NodeId) -> NodeId {
        let rank = self.virtual_topocore.order.rank(node);
        self.core_search.forward_dijkstra.predecessor(rank)
    }
}

pub struct PathServerWrapper<'s, G, O: DijkstraOps<G>, P, Q>(&'s mut Server<G, O, P>, Q);

impl<'s, G, O, P, Q> PathServer for PathServerWrapper<'s, G, O, P, Q>
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

impl<'s, G, O, P, Q> PathServerWrapper<'s, G, O, P, Q>
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
            let dist = self.distance(node);
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

    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.tentative_distance(node)
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.predecessor(node)
    }

    pub fn potential(&self) -> &P {
        &self.0.core_search.potential.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.core_search.potential.potential(node)
    }
}

impl<'s, G: 's, O: 's, P: 's> TDQueryServer<'s, Timestamp, Weight> for Server<G, O, P>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, G, O, P, TDQuery<Timestamp>>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query, |_, _, _, _| ())
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, G: 's, O: 's, P: 's> QueryServer<'s> for Server<G, O, P>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, G, O, P, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query, |_, _, _, _| ())
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

#[derive(Debug, Clone, Copy)]
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

pub struct SkipLowDegServer<Graph = OwnedGraph, Ops = DefaultOps, P = ZeroPotential>
where
    Ops: DijkstraOps<Graph>,
{
    forward_dijkstra: GenTopoDijkstra<Graph, Ops>,
    potential: P,

    reversed: UnweightedOwnedGraph,
    visited: FastClearBitVec,
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential> SkipLowDegServer<Graph, Ops, P>
where
    Graph: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Ops::Arc> + SymmetricDegreeGraph,
{
    pub fn new(graph: Graph, potential: P, ops: Ops) -> Self {
        let n = graph.num_nodes();
        let reversed = UnweightedOwnedGraph::reversed(&graph);
        Self {
            forward_dijkstra: GenTopoDijkstra::new_with_ops(graph, ops),
            potential,

            reversed,
            visited: FastClearBitVec::new(n),
        }
    }

    fn dfs(graph: &UnweightedOwnedGraph, node: NodeId, visited: &mut FastClearBitVec, in_core: &mut impl FnMut(NodeId) -> bool) {
        if visited.get(node as usize) {
            return;
        }
        visited.set(node as usize);
        if in_core(node) {
            return;
        }
        for head in graph.link_iter(node) {
            Self::dfs(graph, head, visited, in_core);
        }
    }

    fn distance(&mut self, query: impl GenQuery<Timestamp> + Copy, inspect: impl FnMut(NodeId, &GenTopoDijkstra<Graph, Ops>, &mut P)) -> Option<Weight> {
        self.forward_dijkstra.initialize_query(query);
        self.potential.init(query.to());
        self.distance_manually_initialized(query, inspect)
    }

    fn distance_manually_initialized(
        &mut self,
        query: impl GenQuery<Timestamp> + Copy,
        mut inspect: impl FnMut(NodeId, &GenTopoDijkstra<Graph, Ops>, &mut P),
    ) -> Option<Weight> {
        report!("algo", "Virtual Topocore Core Query");
        let mut num_queue_pops = 0;

        let departure = *self.forward_dijkstra.tentative_distance(query.from());
        let forward_dijkstra = &mut self.forward_dijkstra;
        let potential = &mut self.potential;

        let target_pot = potential.potential(query.to())?;

        let mut counter = 0;
        self.visited.clear();
        Self::dfs(&self.reversed, query.to(), &mut self.visited, &mut |_| {
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

        while let Some(node) = forward_dijkstra.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, forward_dijkstra, potential);

            if forward_dijkstra
                .queue()
                .peek()
                .map(|e| e.key >= *forward_dijkstra.tentative_distance(query.to()) + target_pot)
                .unwrap_or(false)
            {
                break;
            }
        }

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

    pub fn visualize_query(&mut self, query: impl GenQuery<Timestamp> + Copy, lat: &[f32], lng: &[f32], order: &NodeOrder) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance(query, |node, dijk, pot| {
            let node_id = order.node(node) as usize;
            println!(
                "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                lat[node_id], lng[node_id], num_settled_nodes
            );
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                node_id,
                dijk.tentative_distance(node),
                pot.potential(node).unwrap()
            );
            num_settled_nodes += 1;
        });
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.from() as usize],
            lng[query.from() as usize]
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.to() as usize],
            lng[query.to() as usize]
        );
        res
    }

    fn path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
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

pub struct BiconnectedPathServerWrapper<'s, G, O: DijkstraOps<G>, P, Q>(&'s mut SkipLowDegServer<G, O, P>, Q);

impl<'s, G, O, P, Q> PathServer for BiconnectedPathServerWrapper<'s, G, O, P, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc> + SymmetricDegreeGraph,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        SkipLowDegServer::path(self.0, self.1)
    }
}

impl<'s, G, O, P, Q> BiconnectedPathServerWrapper<'s, G, O, P, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc> + SymmetricDegreeGraph,
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

    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.forward_dijkstra.tentative_distance(node)
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.forward_dijkstra.predecessor(node)
    }

    pub fn potential(&self) -> &P {
        &self.0.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.potential.potential(node)
    }
}

impl<'s, G: 's, O: 's, P: 's> TDQueryServer<'s, Timestamp, Weight> for SkipLowDegServer<G, O, P>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc> + SymmetricDegreeGraph,
{
    type P = BiconnectedPathServerWrapper<'s, G, O, P, TDQuery<Timestamp>>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query, |_, _, _| ())
            .map(move |distance| QueryResult::new(distance, BiconnectedPathServerWrapper(self, query)))
    }
}

impl<'s, G: 's, O: 's, P: 's> QueryServer<'s> for SkipLowDegServer<G, O, P>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc> + SymmetricDegreeGraph,
{
    type P = BiconnectedPathServerWrapper<'s, G, O, P, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query, |_, _, _| ())
            .map(move |distance| QueryResult::new(distance, BiconnectedPathServerWrapper(self, query)))
    }
}
