use super::*;
use crate::algo::{a_star::*, dijkstra::generic_dijkstra::*};
use crate::report::*;

use std::cell::RefCell;

pub struct Server<G, H, P> {
    pub forward: G,
    pub backward: H,
    pub forward_data: DijkstraData<Weight>,
    pub backward_data: DijkstraData<Weight>,
    pub meeting_node: NodeId,
    pub forward_potential: RefCell<P>,
    pub backward_potential: RefCell<P>,
}

impl<G: LinkIterGraph> Server<G, OwnedGraph, ZeroPotential> {
    pub fn new(graph: G) -> Self {
        Self::new_with_potentials(graph, ZeroPotential(), ZeroPotential())
    }
}

impl<G: LinkIterGraph, P: Potential> Server<G, OwnedGraph, P> {
    pub fn new_with_potentials(graph: G, forward_pot: P, backward_pot: P) -> Self {
        let n = graph.num_nodes();
        let reversed = OwnedGraph::reversed(&graph);

        Server {
            forward: graph,
            backward: reversed,
            forward_data: DijkstraData::new(n),
            backward_data: DijkstraData::new(n),
            meeting_node: 0,
            forward_potential: RefCell::new(forward_pot),
            backward_potential: RefCell::new(backward_pot),
        }
    }
}

impl<G: LinkIterGraph, H: LinkIterGraph, P: Potential> Server<G, H, P> {
    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.distance_with_cap(from, to, INFINITY, |_, _, _| ())
    }

    pub fn distance_with_cap(&mut self, from: NodeId, to: NodeId, maximum_distance: Weight, mut inspect: impl FnMut(NodeId, Weight, &mut P)) -> Option<Weight> {
        report!("algo", "Symmetric Bidrectional A* Query");
        // initialize
        let mut tentative_distance = INFINITY;

        let mut fw_ops = DefaultOps();
        let mut bw_ops = DefaultOps();
        let mut forward_dijkstra = DijkstraRun::query(&self.forward, &mut self.forward_data, &mut fw_ops, Query { from, to });
        let mut backward_dijkstra = DijkstraRun::query(&self.backward, &mut self.backward_data, &mut bw_ops, Query { from: to, to: from });

        self.forward_potential.borrow_mut().init(to);
        self.backward_potential.borrow_mut().init(from);

        let mut num_queue_pops = 0;

        let meeting_node = &mut self.meeting_node;
        let forward_potential = &self.forward_potential;
        let backward_potential = &self.backward_potential;

        let result = (|| {
            let mut dir = false;

            while forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) < std::cmp::min(tentative_distance, maximum_distance)
                || backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) < std::cmp::min(tentative_distance, maximum_distance)
            {
                // if forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) <= backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) {
                dir = !dir;
                if dir {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if tentative_distance < INFINITY {
                                if dist + forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) >= tentative_distance {
                                    return false;
                                }
                                let remaining_by_queue = backward_dijkstra
                                    .queue()
                                    .peek()
                                    .map(|q| q.key)
                                    .unwrap_or(INFINITY)
                                    .saturating_sub(backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY));
                                if dist + remaining_by_queue >= tentative_distance {
                                    return false;
                                }
                            }
                            if dist + backward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + backward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            if *backward_dijkstra.tentative_distance(head) < INFINITY && !backward_dijkstra.queue().contains_index(head as usize) {
                                return false;
                            }
                            true
                        },
                        |node| forward_potential.borrow_mut().potential(node),
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *forward_dijkstra.tentative_distance(node), &mut forward_potential.borrow_mut());
                        if node == to {
                            *meeting_node = to;
                            return Some(tentative_distance);
                        }
                    }
                } else {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if tentative_distance < INFINITY {
                                if dist + backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) >= tentative_distance {
                                    return false;
                                }
                                let remaining_by_queue = forward_dijkstra
                                    .queue()
                                    .peek()
                                    .map(|q| q.key)
                                    .unwrap_or(INFINITY)
                                    .saturating_sub(forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY));
                                if dist + remaining_by_queue >= tentative_distance {
                                    return false;
                                }
                            }
                            if dist + forward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + forward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            if *forward_dijkstra.tentative_distance(head) < INFINITY && !forward_dijkstra.queue().contains_index(head as usize) {
                                return false;
                            }
                            true
                        },
                        |node| backward_potential.borrow_mut().potential(node),
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *backward_dijkstra.tentative_distance(node), &mut backward_potential.borrow_mut());
                        if node == from {
                            *meeting_node = from;
                            return Some(tentative_distance);
                        }
                    }
                }
            }

            match tentative_distance {
                INFINITY => None,
                dist => Some(dist),
            }
        })();

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", forward_dijkstra.num_queue_pushs() + backward_dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs() + backward_dijkstra.num_queue_pushs());

        result
    }

    pub fn visualize_query(&mut self, from: NodeId, to: NodeId, lat: &[f32], lng: &[f32]) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance_with_cap(from, to, INFINITY, |node, dist, pot| {
            let node_id = node as usize;
            println!(
                "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                lat[node_id], lng[node_id], num_settled_nodes
            );
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                node_id,
                dist,
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
            let next = self.forward_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to {
            let next = self.backward_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path
    }
}

pub struct PathServerWrapper<'s, G: LinkIterGraph, H: LinkIterGraph, P>(&'s Server<G, H, P>, Query);

impl<'s, G: LinkIterGraph, H: LinkIterGraph, P: Potential> PathServer for PathServerWrapper<'s, G, H, P> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<G: LinkIterGraph, H: LinkIterGraph, P: Potential> QueryServer for Server<G, H, P> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, G, H, P>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
