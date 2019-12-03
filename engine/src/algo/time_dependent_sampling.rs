use super::*;
use crate::algo::customizable_contraction_hierarchy::{customize, query::Server as CCHServer, CCH};
use crate::algo::dijkstra::td_stepped_dijkstra::{QueryProgress, TDSteppedDijkstra};
use crate::datastr::graph::time_dependent::*;
use crate::datastr::graph::RandomLinkAccessGraph;
use crate::datastr::timestamped_vector::TimestampedVector;

use std::ops::Range;

#[derive(Debug)]
pub struct Server<'a> {
    dijkstra: TDSteppedDijkstra,
    samples: Vec<CCHServer<'a>>,
    cch_graph: &'a CCH,
    active_edges: TimestampedVector<bool>,
}

impl<'a> Server<'a> {
    pub fn new(graph: TDGraph, cch: &'a CCH) -> Server<'a> {
        let hour = period() / 24;
        let samples = vec![
            Range { start: 22, end: 5 },
            Range { start: 7, end: 10 },
            Range { start: 11, end: 15 },
            Range { start: 16, end: 19 },
        ]
        .into_iter()
        .map(|range| {
            let range = Range {
                start: range.start * hour,
                end: range.end * hour,
            };
            WrappingRange::new(range)
        })
        .map(|range| {
            (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).average(range.clone()))
                .collect::<Vec<Weight>>()
        })
        .map(|metric| CCHServer::new(customize(cch, &FirstOutGraph::new(graph.first_out(), graph.head(), metric))))
        .collect();

        Server {
            active_edges: TimestampedVector::new(graph.num_arcs(), false),
            dijkstra: TDSteppedDijkstra::new(graph),
            samples,
            cch_graph: cch,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        self.active_edges.reset();

        for server in &mut self.samples {
            let result = server.query(Query {
                from: self.cch_graph.node_order().rank(from),
                to: self.cch_graph.node_order().rank(to),
            });
            if let Some(mut result) = result {
                let path = result.path();
                let path_iter = path.iter();
                let mut second_node_iter = path_iter.clone();
                second_node_iter.next();

                for (first_node, second_node) in path_iter.zip(second_node_iter) {
                    self.active_edges[self.dijkstra.graph().edge_index(*first_node, *second_node).unwrap() as usize] = true;
                }
            }
        }

        self.dijkstra.initialize_query(TDQuery { from, to, departure });

        loop {
            let active_edges = &self.active_edges;
            match self.dijkstra.next_step(|edge_id| active_edges[edge_id as usize], |_| 0) {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result,
            }
        }
    }

    fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.dijkstra.query().to);

        while *path.last().unwrap() != self.dijkstra.query().from {
            let next = self.dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>);

impl<'s, 'a> PathServer<'s> for PathServerWrapper<'s, 'a> {
    type NodeInfo = NodeId;

    fn path(&'s mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, 'a: 's> TDQueryServer<'s, Timestamp, Weight> for Server<'a> {
    type P = PathServerWrapper<'s, 'a>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure).map(move |distance| QueryResult {
            distance,
            path_server: PathServerWrapper(self),
        })
    }
}
