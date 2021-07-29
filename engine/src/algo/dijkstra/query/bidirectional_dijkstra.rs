use super::*;
use crate::algo::{a_star::*, dijkstra::generic_dijkstra::*};

use std::{cell::RefCell, cmp::min};

pub struct Server<G, H, P = BiDirZeroPot, D = ChooseMinKeyDir> {
    pub forward: G,
    pub backward: H,
    pub forward_data: DijkstraData<Weight>,
    pub backward_data: DijkstraData<Weight>,
    pub meeting_node: NodeId,
    pub potential: P,
    pub dir_chooser: D,
}

impl<G: LinkIterGraph, D: BidirChooseDir> Server<G, OwnedGraph, BiDirZeroPot, D> {
    pub fn new(graph: G) -> Self {
        Self::new_with_potentials(graph, BiDirZeroPot)
    }
}

impl<G: LinkIterGraph, P: BiDirPotential, D: BidirChooseDir> Server<G, OwnedGraph, P, D> {
    pub fn new_with_potentials(graph: G, potential: P) -> Self {
        let n = graph.num_nodes();
        let reversed = OwnedGraph::reversed(&graph);

        Server {
            forward: graph,
            backward: reversed,
            forward_data: DijkstraData::new(n),
            backward_data: DijkstraData::new(n),
            meeting_node: 0,
            potential,
            dir_chooser: Default::default(),
        }
    }
}

impl<G: LinkIterGraph, H: LinkIterGraph, P: BiDirPotential, D: BidirChooseDir> Server<G, H, P, D> {
    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.distance_with_cap(from, to, INFINITY, |_, _, _| ())
    }

    pub fn distance_with_cap(&mut self, from: NodeId, to: NodeId, maximum_distance: Weight, mut inspect: impl FnMut(NodeId, Weight, &mut P)) -> Option<Weight> {
        report!("algo", "Bidrectional Dijkstra Query");
        D::report();
        P::report();
        // initialize
        let mut tentative_distance = INFINITY;

        let mut fw_ops = DefaultOps();
        let mut bw_ops = DefaultOps();
        let mut forward_dijkstra = DijkstraRun::query(&self.forward, &mut self.forward_data, &mut fw_ops, Query { from, to });
        let mut backward_dijkstra = DijkstraRun::query(&self.backward, &mut self.backward_data, &mut bw_ops, Query { from: to, to: from });

        self.potential.init(from, to);

        let mut num_queue_pops = 0;
        let meeting_node = &mut self.meeting_node;
        let mut potential = RefCell::new(&mut self.potential);
        let dir_chooser = &mut self.dir_chooser;

        let result = (|| {
            while !potential.get_mut().stop(
                forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                min(tentative_distance, maximum_distance),
            ) {
                if dir_chooser.choose(
                    forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                    backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                ) {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if potential.borrow_mut().prune_forward(
                                NodeIdT(head),
                                dist,
                                backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                                min(tentative_distance, maximum_distance),
                            ) {
                                return false;
                            }
                            if dist + backward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + backward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| potential.borrow_mut().forward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *forward_dijkstra.tentative_distance(node), &mut potential.borrow_mut());
                        if node == to {
                            *meeting_node = to;
                            return Some(tentative_distance);
                        }
                    } else {
                        return None;
                    }
                } else {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            if potential.borrow_mut().prune_backward(
                                NodeIdT(head),
                                dist,
                                forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                                min(tentative_distance, maximum_distance),
                            ) {
                                return false;
                            }
                            if dist + forward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + forward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| potential.borrow_mut().backward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        inspect(node, *backward_dijkstra.tentative_distance(node), &mut potential.borrow_mut());
                        if node == from {
                            *meeting_node = from;
                            return Some(tentative_distance);
                        }
                    } else {
                        return None;
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
                "marker.bindPopup(\"id: {}<br>distance: {}<br>forward_potential: {}<br>backward_potential: {}\");",
                node_id,
                dist,
                pot.forward_potential(node_id as NodeId).unwrap(),
                pot.backward_potential(node_id as NodeId).unwrap(),
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

    fn node_path(&self, query: Query) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != query.from {
            let next = self.forward_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to {
            let next = self.backward_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path
    }
}

pub struct PathServerWrapper<'s, G: LinkIterGraph, H: LinkIterGraph, P, D>(&'s Server<G, H, P, D>, Query);

impl<'s, G: LinkIterGraph, H: LinkIterGraph, P: BiDirPotential, D: BidirChooseDir> PathServer for PathServerWrapper<'s, G, H, P, D> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::node_path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<G: LinkIterGraph, H: LinkIterGraph, P: BiDirPotential, D: BidirChooseDir> QueryServer for Server<G, H, P, D> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, G, H, P, D>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self, query))
    }
}
