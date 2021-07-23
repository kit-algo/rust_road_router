//! CATCHUp query algorithm.

use crate::algo::customizable_contraction_hierarchy::*;
use crate::algo::{dijkstra::State, *};
use crate::datastr::clearlist_vector::ClearlistVector;
use crate::datastr::graph::floating_time_dependent::ShortcutId;
use crate::datastr::graph::time_dependent::*;
use crate::datastr::index_heap::{IndexdMinHeap, Indexing};
use crate::datastr::rank_select_map::{BitVec, FastClearBitVec};
#[cfg(feature = "tdcch-query-detailed-timing")]
use crate::report::benchmark::Timer;

pub use crate::algo::customizable_contraction_hierarchy::catchup_light::*;

use super::td_stepped_elimination_tree::{QueryProgress, *};

use crate::report::*;
use std::cmp::*;

/// Query server struct for CATCHUp.
/// Implements the common query trait.
pub struct Server<'a> {
    // Corridor elimination tree query
    forward: TDSteppedEliminationTree<'a, 'a>,
    backward: TDSteppedEliminationTree<'a, 'a>,
    // static CCH stuff
    cch_graph: &'a CCH,
    // CATCHUp preprocessing
    customized_graph: &'a CustomizedGraph<'a>,

    // Middle nodes in corridor
    meeting_nodes: Vec<(NodeId, Weight)>,

    // For storing the paths through the elimination tree from the root to source/destination.
    // Because elimination only allows us to efficiently walk from node to root, but not the other way around
    forward_tree_path: Vec<NodeId>,
    backward_tree_path: Vec<NodeId>,
    // bitsets to mark the subset of nodes (subset of tree_path) in the elimination tree shortest path corridor
    backward_tree_mask: BitVec,
    forward_tree_mask: BitVec,

    // Distances for Dijkstra/A* phase
    distances: ClearlistVector<Timestamp>,
    // Distances estimates to target used as A* potentials
    lower_bounds_to_target: ClearlistVector<Weight>,
    upper_bounds_from_source: ClearlistVector<Weight>,
    // Parent pointers for path retrieval, including edge id for easier shortcut unpacking
    parents: Vec<(NodeId, EdgeId)>,
    // Priority queue for Dijkstra/A* phase
    closest_node_priority_queue: IndexdMinHeap<State<Timestamp>>,
    // Bitset to mark all (upward) edges in the search space
    relevant_upward: FastClearBitVec,

    down_relaxed: FastClearBitVec,

    from: NodeId,
    to: NodeId,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCH, customized_graph: &'a CustomizedGraph<'a>) -> Self {
        let n = customized_graph.original_graph.num_nodes();
        let m = cch_graph.num_arcs();
        Self {
            forward: TDSteppedEliminationTree::new(customized_graph.upward_bounds_graph(), cch_graph.elimination_tree()),
            backward: TDSteppedEliminationTree::new(customized_graph.downward_bounds_graph(), cch_graph.elimination_tree()),
            cch_graph,
            meeting_nodes: Vec::new(),
            customized_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
            distances: ClearlistVector::new(n, INFINITY),
            lower_bounds_to_target: ClearlistVector::new(n, INFINITY),
            upper_bounds_from_source: ClearlistVector::new(n, INFINITY),
            parents: vec![(std::u32::MAX, std::u32::MAX); n],
            forward_tree_mask: BitVec::new(n),
            backward_tree_mask: BitVec::new(n),
            closest_node_priority_queue: IndexdMinHeap::new(n),
            relevant_upward: FastClearBitVec::new(m),
            down_relaxed: FastClearBitVec::new(m),
            from: 0,
            to: 0,
        }
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    fn distance(&mut self, from_node: NodeId, to_node: NodeId, departure_time: Timestamp) -> Option<Weight> {
        report!("algo", "CATCHUp light Query");

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let timer = Timer::new();

        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.customized_graph.original_graph.num_nodes();

        // initialize
        let mut tentative_distance = (INFINITY, INFINITY);
        self.distances.reset();
        self.distances[self.from as usize] = departure_time;
        self.lower_bounds_to_target.reset();
        self.upper_bounds_from_source.reset();
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);
        self.closest_node_priority_queue.clear();
        self.relevant_upward.clear();

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let init_time = timer.get_passed();

        // stats
        let mut nodes_in_elimination_tree_search_space = 0;
        let mut relaxed_elimination_tree_arcs = 0;
        let mut relaxed_shortcut_arcs = 0;
        let mut num_settled_nodes = 0;

        // elimination tree corridor query
        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            // advance the direction which currently is at the lower rank
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                // while we're here, clean up the forward_tree_mask
                // this is fine because we only check the flag for nodes on the tree path
                // we only set this to true when the node is guaranteed to be in the corridor.
                self.forward_tree_mask.unset_all_around(self.forward.peek_next().unwrap() as usize);

                // experimental stall on demand
                // only works for forward search
                // currently deactivated by default
                let stall = || {
                    for ((target, _), (_, shortcut_upper_bound)) in
                        self.customized_graph.downward_bounds_graph().neighbor_iter(self.forward.peek_next().unwrap())
                    {
                        if self.forward.node_data(target).upper_bound + shortcut_upper_bound
                            < self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound
                        {
                            return true;
                        }
                    }
                    false
                };

                // skip the node if it cannot possibly be in the shortest path corridor
                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > tentative_distance.1
                    || (cfg!(feature = "tdcch-stall-on-demand") && stall())
                {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    if cfg!(feature = "detailed-stats") {
                        nodes_in_elimination_tree_search_space += 1;
                    }
                    if cfg!(feature = "detailed-stats") {
                        relaxed_elimination_tree_arcs += self.customized_graph.upward_bounds_graph().degree(node);
                    }

                    // push node to tree path so we can efficiently walk back down later
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    let upper_bound = self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound;

                    // we use this distance later for some pruning
                    // so here, we already store upper_bound + epsilon as a distance
                    // this allows for pruning but guarantees that we will later improve it with a real distance, even if its exactly upper_bound
                    self.distances[node as usize] = min(self.distances[node as usize], (departure_time + 1).saturating_add(upper_bound));

                    // improve tentative distance if possible
                    if lower_bound <= tentative_distance.1 {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(tentative_distance.1, upper_bound);
                        debug_assert!(tentative_distance.0 <= tentative_distance.1, "{:?}", tentative_distance);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    unreachable!("inconsistent elimination tree state");
                }
            } else {
                // while we're here, clean up the forward_tree_mask
                // this is fine because we only check the flag for nodes on the tree path
                // we only set this to true when the node is guaranteed to be in the corridor.
                self.backward_tree_mask.unset_all_around(self.backward.peek_next().unwrap() as usize);

                // skip the node if it cannot possibly be in the shortest path corridor
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
                    if cfg!(feature = "detailed-stats") {
                        nodes_in_elimination_tree_search_space += 1;
                    }
                    if cfg!(feature = "detailed-stats") {
                        relaxed_elimination_tree_arcs += self.customized_graph.downward_bounds_graph().degree(node);
                    }

                    // push node to tree path so we can efficiently walk back down later
                    self.backward_tree_path.push(node);

                    // improve tentative distance if possible
                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if lower_bound <= tentative_distance.1 {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(
                            tentative_distance.1,
                            self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound,
                        );
                        debug_assert!(tentative_distance.0 <= tentative_distance.1, "{:?}", tentative_distance);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    unreachable!("inconsistent elimination tree state");
                }
            }
        }

        if cfg!(feature = "detailed-stats") {
            report!("num_nodes_in_elimination_tree_search_space", nodes_in_elimination_tree_search_space);
            report!("num_relaxed_elimination_tree_arcs", relaxed_elimination_tree_arcs);
            report!("num_meeting_nodes", self.meeting_nodes.len());
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let elimination_tree_time = timer.get_passed();

        // elimination tree query done, now we want to retrieve the corridor

        let tentative_upper_bound = tentative_distance.1;
        let tentative_latest_arrival = departure_time + tentative_upper_bound;

        // all meeting nodes are in the corridor
        for &(node, _) in self
            .meeting_nodes
            .iter()
            .filter(|&&(_, lower_bound)| lower_bound <= tentative_upper_bound && lower_bound < INFINITY)
        {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        // for all nodes on the path from root to target
        for &node in self.backward_tree_path.iter().rev() {
            // if the node is in the corridor
            if self.backward_tree_mask.get(node as usize) {
                // set lower bound to target for the A* potentials later on
                self.lower_bounds_to_target[node as usize] = self.backward.node_data(node).lower_bound;
                let upper_bound = self.backward.node_data(node).upper_bound;

                // for all labels (parent pointers)
                for label in self
                    .backward
                    .node_data(node)
                    .labels
                    .iter()
                    // lazy label filtering
                    .filter(|label| label.lower_bound <= upper_bound)
                {
                    // mark parent as in corridor
                    self.backward_tree_mask.set(label.parent as usize);
                }
            }
        }

        // for all nodes on the path from root to origin
        while let Some(node) = self.forward_tree_path.pop() {
            // if the node is in the corridor
            if self.forward_tree_mask.get(node as usize) {
                // for all nodes that were also in the backward search space,
                // (the ones where forward and backward path overlap)
                // we already know a lower bound to target.
                // we are going to propagate this downward the corridor towards the source and all nodes on the way
                let reverse_lower = self.lower_bounds_to_target[node as usize];
                let upper_bound = self.forward.node_data(node).upper_bound;
                self.upper_bounds_from_source[node as usize] = upper_bound + departure_time;

                // for all labels (parent points)
                for label in self
                    .forward
                    .node_data(node)
                    .labels
                    .iter()
                    // lazy label filtering
                    .filter(|label| label.lower_bound <= upper_bound)
                {
                    // update (relax) parent lower bound to target
                    self.lower_bounds_to_target[label.parent as usize] = min(
                        self.lower_bounds_to_target[label.parent as usize],
                        reverse_lower + label.lower_bound - self.forward.node_data(label.parent).lower_bound,
                    );
                    // mark parent as in corridor
                    self.forward_tree_mask.set(label.parent as usize);
                    // mark edge as in search space
                    self.relevant_upward.set(label.shortcut_id as usize);
                }
            }
        }

        for &node in self.backward_tree_path.iter().rev() {
            // if the node is in the corridor
            if self.backward_tree_mask.get(node as usize) {
                let reverse_upper = self.upper_bounds_from_source[node as usize];
                let upper_bound = self.backward.node_data(node).upper_bound;

                // for all labels (parent pointers)
                for label in self
                    .backward
                    .node_data(node)
                    .labels
                    .iter()
                    // lazy label filtering
                    .filter(|label| label.lower_bound <= upper_bound)
                {
                    self.upper_bounds_from_source[label.parent as usize] = min(
                        self.upper_bounds_from_source[label.parent as usize],
                        reverse_upper + self.customized_graph.downward_bounds_graph().bounds[label.shortcut_id as usize].1,
                    );
                }
            }
        }
        self.backward_tree_path.clear();

        // now we have the corridor so we proceed to the Dijkstra/A* phase

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let forward_select_time = timer.get_passed();

        let relevant_upward = &mut self.relevant_upward;
        // debug_assert!(
        //     tentative_distance.0.fuzzy_eq(self.lower_bounds_to_target[self.from as usize]),
        //     "{:?}",
        //     dbg_each!(tentative_distance, self.lower_bounds_to_target[self.from as usize])
        // );
        debug_assert!(
            self.lower_bounds_to_target[self.to as usize] == 0 || tentative_distance.0 == INFINITY,
            "{:?}",
            dbg_each!(self.lower_bounds_to_target[self.to as usize])
        );
        let lower_bounds_to_target = &mut self.lower_bounds_to_target;
        let upper_bounds_from_source = &mut self.upper_bounds_from_source;
        let distances = &mut self.distances;
        let closest_node_priority_queue = &mut self.closest_node_priority_queue;
        let parents = &mut self.parents;

        closest_node_priority_queue.push(State {
            key: departure_time + tentative_distance.0,
            node: self.from,
        });

        // while there is a node in the queue
        while let Some(State { node, .. }) = closest_node_priority_queue.pop() {
            self.down_relaxed.clear();
            // dbg!(node);
            if cfg!(feature = "detailed-stats") {
                num_settled_nodes += 1;
            }

            let distance = distances[node as usize];

            if node == self.to {
                break;
            }

            // for each outgoing (forward) edge
            for ((target, shortcut_id), (shortcut_lower_bound, _shortcut_upper_bound)) in self.customized_graph.upward_bounds_graph().neighbor_iter(node) {
                // if its in the search space
                // and can improve the distance of the target according to the bounds
                if relevant_upward.get(shortcut_id as usize) && distance + shortcut_lower_bound <= min(tentative_latest_arrival, distances[target as usize]) {
                    self.customized_graph.evaluate_next_segment_at(
                        ShortcutId::Outgoing(shortcut_id),
                        distance,
                        lower_bounds_to_target,
                        upper_bounds_from_source,
                        &mut self.down_relaxed,
                        &mut |edge_id| relevant_upward.set(edge_id as usize),
                        &mut |tt, NodeIdT(next_on_path), evaled_shortcut_id, lower| {
                            if cfg!(feature = "detailed-stats") {
                                relaxed_shortcut_arcs += 1;
                            }

                            let next_ea = distance + tt;
                            let next = State {
                                key: next_ea + lower,
                                node: next_on_path,
                            };

                            // actual distance relaxation for `next_on_path`
                            // we need to call decrease_key when either the lower bound or the distance was improved
                            if next_ea < distances[next.node as usize] {
                                distances[next.node as usize] = next_ea;
                                parents[next.node as usize] = (node, evaled_shortcut_id.edge_id());
                                if closest_node_priority_queue.contains_index(next.as_index()) {
                                    closest_node_priority_queue.decrease_key(next);
                                } else {
                                    closest_node_priority_queue.push(next);
                                }
                            } else if let Some(label) = closest_node_priority_queue.get(next.as_index()) {
                                if next < *label {
                                    closest_node_priority_queue.decrease_key(next);
                                }
                            }
                        },
                    );
                }
            }

            // for most nodes we relax down edges implicitly through the evaluate_next_segment_at method.
            // but when we have a node in the downward elimination tree corridor we also need to relax downward shortcuts
            if self.backward_tree_mask.get(node as usize) {
                let upper_bound = self.backward.node_data(node).upper_bound;

                // for all labels
                for label in self.backward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    // check by bounds if we need the edge
                    if distance + self.customized_graph.downward_bounds_graph().bounds[label.shortcut_id as usize].0
                        <= min(tentative_latest_arrival, distances[label.parent as usize])
                    {
                        self.customized_graph.evaluate_next_segment_at(
                            ShortcutId::Incoming(label.shortcut_id),
                            distance,
                            lower_bounds_to_target,
                            upper_bounds_from_source,
                            &mut self.down_relaxed,
                            &mut |edge_id| relevant_upward.set(edge_id as usize),
                            &mut |tt, NodeIdT(next_on_path), evaled_shortcut_id, lower| {
                                if cfg!(feature = "detailed-stats") {
                                    relaxed_shortcut_arcs += 1;
                                }

                                let next_ea = distance + tt;
                                let next = State {
                                    key: next_ea + lower,
                                    node: next_on_path,
                                };

                                // actual distance relaxation for `next_on_path`
                                // we need to call decrease_key when either the lower bound or the distance was improved
                                if next_ea < distances[next.node as usize] {
                                    distances[next.node as usize] = next_ea;
                                    parents[next.node as usize] = (node, evaled_shortcut_id.edge_id());
                                    if closest_node_priority_queue.contains_index(next.as_index()) {
                                        closest_node_priority_queue.decrease_key(next);
                                    } else {
                                        closest_node_priority_queue.push(next);
                                    }
                                } else if let Some(label) = closest_node_priority_queue.get(next.as_index()) {
                                    if next < *label {
                                        closest_node_priority_queue.decrease_key(next);
                                    }
                                }
                            },
                        );
                    }
                }
            }
        }

        if cfg!(feature = "detailed-stats") {
            report!("num_relaxed_shortcut_arcs", relaxed_shortcut_arcs);
            report!("num_settled_nodes", num_settled_nodes);
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let relax_time = timer.get_passed();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        {
            eprintln!(
                "elimination tree: {} {:?}%",
                elimination_tree_time - init_time,
                100 * ((elimination_tree_time - init_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
            eprintln!(
                "fw select: {} {:?}%",
                forward_select_time - elimination_tree_time,
                100 * ((forward_select_time - elimination_tree_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
            eprintln!(
                "relax: {} {:?}%",
                relax_time - forward_select_time,
                100 * ((relax_time - forward_select_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
        }

        if self.distances[self.to as usize] < INFINITY {
            Some(self.distances[self.to as usize] - departure_time)
        } else {
            None
        }
    }

    fn path(&self) -> Vec<(NodeId, Timestamp)> {
        unimplemented!()
        // let mut path = Vec::new();
        // path.push((self.to, self.distances[self.to as usize]));

        // while let Some((rank, t_prev)) = path.pop() {
        //     debug_assert_eq!(t_prev, self.distances[rank as usize]);
        //     if rank == self.from {
        //         path.push((rank, t_prev));
        //         break;
        //     }

        //     let (parent, shortcut_id) = self.parents[rank as usize];
        //     let t_parent = self.distances[parent as usize];

        //     let mut shortcut_path = Vec::new();
        //     if parent > rank {
        //         self.customized_graph
        //             .incoming
        //             .unpack_at(shortcut_id, t_parent, &self.customized_graph, &mut shortcut_path);
        //     } else {
        //         self.customized_graph
        //             .outgoing
        //             .unpack_at(shortcut_id, t_parent, &self.customized_graph, &mut shortcut_path);
        //     };

        //     for (edge, arrival) in shortcut_path.into_iter().rev() {
        //         path.push((
        //             self.cch_graph.node_order().rank(self.customized_graph.original_graph.head()[edge as usize]),
        //             arrival,
        //         ));
        //     }

        //     path.push((parent, t_parent));
        // }

        // path.reverse();

        // for (rank, _) in &mut path {
        //     *rank = self.cch_graph.node_order().node(*rank);
        // }

        // path
    }
}

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>);

impl<'s, 'a> PathServer for PathServerWrapper<'s, 'a> {
    type NodeInfo = (NodeId, Timestamp);
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<'a> TDQueryServer<Timestamp, Weight> for Server<'a> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, 'a>;

    fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to, query.departure), PathServerWrapper(self))
    }
}
