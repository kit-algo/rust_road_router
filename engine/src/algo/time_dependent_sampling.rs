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
        dijkstra::{generic_dijkstra::*, query::td_dijkstra::TDDijkstraOps, *},
    },
    datastr::{graph::time_dependent::*, graph::RandomLinkAccessGraph, timestamped_vector::TimestampedVector},
};

use std::ops::Range;

/// Query server struct for TD-S.
/// Implements the common query trait.
pub struct Server<'a> {
    graph: TDGraph,
    // The Dijkstra algo on the original graph
    dijkstra_data: DijkstraData<Weight>,
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
            dijkstra_data: DijkstraData::new(graph.num_nodes()),
            samples,
            graph,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        self.active_edges.reset();

        // query each window independently and mark edges
        for server in &mut self.samples {
            let mut result = server.query(Query { from, to });
            if let Some(path) = result.node_path() {
                for edge in path.windows(2) {
                    self.active_edges[self.graph.edge_index(edge[0], edge[1]).unwrap() as usize] = true;
                }
            }
        }

        // dijkstra on subgraph
        let mut ops = TDDijkstraOps();
        let mut dijkstra = DijkstraRun::query(&self.graph, &mut self.dijkstra_data, &mut ops, TDQuery { from, to, departure });

        let active_edges = &self.active_edges;
        while let Some(node) = dijkstra.next_filtered_edges(|&(_, edge_id)| active_edges[edge_id.0 as usize]) {
            if node == to {
                return Some(dijkstra.tentative_distance(node) - departure);
            }
        }

        None
    }

    fn path(&self, query: TDQuery<Weight>) -> Vec<NodeId> {
        self.dijkstra_data.node_path(query.from, query.to)
    }
}

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>, TDQuery<Weight>);

impl<'s, 'a> PathServer for PathServerWrapper<'s, 'a> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        self.0.dijkstra_data.edge_path(self.1.from, self.1.to)
    }
}

impl<'a> TDQueryServer<Timestamp, Weight> for Server<'a> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, 'a>;

    fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to, query.departure), PathServerWrapper(self, query))
    }
}
