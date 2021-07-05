use super::*;
use crate::algo::{a_star::*, dijkstra::generic_dijkstra::*};
use crate::report::*;

use std::cell::RefCell;

pub struct Server<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>, P> {
    pub forward_dijkstra: GenericDijkstra<G>,
    pub backward_dijkstra: GenericDijkstra<H>,
    pub tentative_distance: Weight,
    pub meeting_node: NodeId,
    pub potential: RefCell<AveragePotential<P, P>>,
}

impl<G: for<'a> LinkIterGraph<'a>> Server<G, OwnedGraph, ZeroPotential> {
    pub fn new(graph: G) -> Self {
        Self::new_with_potentials(graph, ZeroPotential(), ZeroPotential())
    }
}

impl<G: for<'a> LinkIterGraph<'a>, P: Potential> Server<G, OwnedGraph, P> {
    pub fn new_with_potentials(graph: G, forward_pot: P, backward_pot: P) -> Self {
        let reversed = OwnedGraph::reversed(&graph);

        Server {
            forward_dijkstra: GenericDijkstra::new(graph),
            backward_dijkstra: GenericDijkstra::new(reversed),
            tentative_distance: INFINITY,
            meeting_node: 0,
            potential: RefCell::new(AveragePotential::new(forward_pot, backward_pot)),
        }
    }
}

impl<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>, P: Potential> Server<G, H, P> {
    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.distance_with_cap(from, to, INFINITY, |_, _, _, _| ())
    }

    pub fn distance_with_cap(
        &mut self,
        from: NodeId,
        to: NodeId,
        maximum_distance: Weight,
        mut inspect: impl FnMut(NodeId, Weight, &mut AveragePotential<P, P>, i32),
    ) -> Option<Weight> {
        report!("algo", "Bidrectional Dijkstra Query");
        // initialize
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        self.potential.borrow_mut().init(from, to);

        let mut num_queue_pops = 0;

        let result = (|| {
            let forward_add = (self.potential.borrow_mut().backward_potential(to)? / 2) as i32;
            let backward_add = (self.potential.borrow_mut().forward_potential(from)? / 2) as i32;
            let mu_add = (self.potential.borrow_mut().potential(from)? + forward_add) as Weight;

            let forward_dijkstra = &mut self.forward_dijkstra;
            let backward_dijkstra = &mut self.backward_dijkstra;
            let meeting_node = &mut self.meeting_node;
            let tentative_distance = &mut self.tentative_distance;
            let potential = &self.potential;

            while forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) + backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY)
                < std::cmp::min(*tentative_distance, maximum_distance) + mu_add
            {
                let searches_met = *tentative_distance < INFINITY;
                if forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) <= backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if searches_met && dist + potential.borrow_mut().forward_potential(head).unwrap_or(INFINITY) >= *tentative_distance {
                                return false;
                            }
                            if dist + backward_dijkstra.tentative_distance(head) < *tentative_distance {
                                *tentative_distance = dist + backward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| {
                            if searches_met {
                                potential.borrow_mut().potential(node).map(|p| (p + forward_add) as Weight)
                            } else {
                                potential.borrow_mut().forward_potential(node)
                            }
                        },
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *forward_dijkstra.tentative_distance(node), &mut potential.borrow_mut(), forward_add);
                        if node == to {
                            self.meeting_node = to;
                            return Some(*tentative_distance);
                        }
                    } else {
                        return None;
                    }
                } else {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if searches_met && dist + potential.borrow_mut().backward_potential(head).unwrap_or(INFINITY) >= *tentative_distance {
                                return false;
                            }
                            if dist + forward_dijkstra.tentative_distance(head) < *tentative_distance {
                                *tentative_distance = dist + forward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| {
                            if searches_met {
                                potential.borrow_mut().potential(node).map(|p| (backward_add - p) as Weight)
                            } else {
                                potential.borrow_mut().backward_potential(node)
                            }
                        },
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *backward_dijkstra.tentative_distance(node), &mut potential.borrow_mut(), -backward_add);
                        if node == from {
                            self.meeting_node = from;
                            return Some(*tentative_distance);
                        }
                    } else {
                        return None;
                    }
                }
                if !searches_met && *tentative_distance < INFINITY {
                    forward_dijkstra.exchange_potential(|node| potential.borrow_mut().potential(node).map(|p| (p + forward_add) as Weight));
                    backward_dijkstra.exchange_potential(|node| potential.borrow_mut().potential(node).map(|p| (backward_add - p) as Weight));
                }
            }

            match self.tentative_distance {
                INFINITY => None,
                dist => Some(dist),
            }
        })();

        report!("num_queue_pops", num_queue_pops);
        report!(
            "num_queue_pushs",
            self.forward_dijkstra.num_queue_pushs() + self.backward_dijkstra.num_queue_pushs()
        );
        report!(
            "num_relaxed_arcs",
            self.forward_dijkstra.num_relaxed_arcs() + self.backward_dijkstra.num_queue_pushs()
        );

        result
    }

    pub fn visualize_query(&mut self, from: NodeId, to: NodeId, lat: &[f32], lng: &[f32]) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance_with_cap(from, to, INFINITY, |node, dist, pot, pot_add| {
            let node_id = node as usize;
            println!(
                "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                lat[node_id], lng[node_id], num_settled_nodes
            );
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}<br>ugly_pot: {}\");",
                node_id,
                dist,
                pot.potential(node_id as NodeId).unwrap() + pot_add,
                pot.potential(node_id as NodeId).unwrap()
            );
            num_settled_nodes += 1;
        });
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[from as usize], lng[from as usize]
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[to as usize], lng[to as usize]
        );
        res
    }

    fn path(&self, query: Query) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != query.from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to {
            let next = self.backward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path
    }
}

pub struct PathServerWrapper<'s, G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>, P>(&'s Server<G, H, P>, Query);

impl<'s, G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>, P: Potential> PathServer for PathServerWrapper<'s, G, H, P> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>, P: Potential> QueryServer for Server<G, H, P> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, G, H, P>;

    fn query(&mut self, query: Query) -> Option<QueryResult<Self::P<'_>, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
