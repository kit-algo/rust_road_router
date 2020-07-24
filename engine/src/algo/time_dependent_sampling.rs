//! Time-dependent sampling implementation based on CCHs
//!
//! TD-S is a simple heuristic for time-dependent routing.
//! During preprocessing the time-dependent functions are split into a couple of windows.
//! For each window one static travel time is determined for each link, i.E. by taking the average of the travel times in that window.
//! The query algorithm is to run independent fast shortest path queries on each window and combine the resulting optimal paths into a subgraph of the original graph.
//! On this subgraph, a standard time-dependent dijkstra is performed.
//!
//! Preprocessing is completely done in CCHs, so this module is actually just the query algorithm.

use super::*;
use crate::{
    algo::{
        customizable_contraction_hierarchy::{customize, query::Server as CCHServer, CCH},
        dijkstra::{generic_dijkstra::*, query::td_dijkstra::TDDijkstraOps},
    },
    datastr::{graph::time_dependent::*, graph::RandomLinkAccessGraph, timestamped_vector::TimestampedVector},
};

use std::ops::Range;

/// Query server struct for TD-S.
/// Implements the common query trait.
pub struct Server<'a> {
    // The Dijkstra algo on the original graph
    dijkstra: GenericDijkstra<TDDijkstraOps, TDGraph>,
    // A CCH Server for each time window
    samples: Vec<CCHServer<'a, CCH>>,
    // marking edges in the subgraph we perform dijkstra on
    active_edges: TimestampedVector<bool>,
}

impl<'a> Server<'a> {
    pub fn new(graph: TDGraph, cch: &'a CCH) -> Server<'a> {
        let hour = period() / 24; // here, we assume, that our travel time functions cover one day
                                  // hardcoded four time windows
        let samples = [
            Range { start: 22, end: 5 },
            Range { start: 7, end: 10 },
            Range { start: 11, end: 15 },
            Range { start: 16, end: 19 },
        ]
        .iter()
        .map(|range| {
            let range = Range {
                start: range.start * hour,
                end: range.end * hour,
            };
            WrappingRange::new(range)
        })
        .map(|range| {
            // average travel time for each window
            (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).average(range.clone()))
                .collect::<Vec<Weight>>()
        })
        .map(|metric| CCHServer::new(customize(cch, &FirstOutGraph::new(graph.first_out(), graph.head(), metric)))) // customize CCH for each window
        .collect();

        Server {
            active_edges: TimestampedVector::new(graph.num_arcs(), false),
            dijkstra: GenericDijkstra::new(graph),
            samples,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        self.active_edges.reset();

        // query each window independently and mark edges
        for server in &mut self.samples {
            let result = server.query(Query { from, to });
            if let Some(mut result) = result {
                for edge in result.path().windows(2) {
                    self.active_edges[self.dijkstra.graph().edge_index(edge[0], edge[1]).unwrap() as usize] = true;
                }
            }
        }

        // dijkstra on subgraph
        self.dijkstra.initialize_query(TDQuery { from, to, departure });

        let active_edges = &self.active_edges;
        while let Some(node) = self.dijkstra.next_filtered_edges(|&(_, edge_id)| active_edges[edge_id as usize]) {
            if node == to {
                return Some(*self.dijkstra.tentative_distance(node) - departure);
            }
        }

        None
    }

    fn path(&self, query: TDQuery<Weight>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to);

        while *path.last().unwrap() != query.from {
            let next = self.dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>, TDQuery<Weight>);

impl<'s, 'a> PathServer for PathServerWrapper<'s, 'a> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, 'a: 's> TDQueryServer<'s, Timestamp, Weight> for Server<'a> {
    type P = PathServerWrapper<'s, 'a>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
