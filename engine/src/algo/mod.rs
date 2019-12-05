//! Building blocks for fast routing algorithms.

use crate::datastr::graph::*;

use self::dijkstra::stepped_dijkstra::{QueryProgress, State, SteppedDijkstra};

pub mod catchup;
pub mod ch_potentials;
pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod dijkstra;
pub mod time_dependent_sampling;
pub mod topocore;

/// Simply a source-target pair
#[derive(Debug, Clone, Copy)]
pub struct Query {
    pub from: NodeId,
    pub to: NodeId,
}

/// A source-target pair with a departure time.
/// Genric over the timestamp type, so we can support both integer and float weights
#[derive(Debug, Clone, Copy)]
pub struct TDQuery<T: Copy> {
    pub from: NodeId,
    pub to: NodeId,
    pub departure: T,
}

/// Generic container for query results.
/// Since queries usually modify the state of the internal algorithm data structures,
/// it is usually impossible to retrieve a path for an older query result once a new query was performed.
/// This type use rusts lifetimes to enforce this behaviour through the method signatures.
#[derive(Debug)]
pub struct QueryResult<P, W> {
    distance: W,
    path_server: P,
}

impl<'s, P, W: Copy> QueryResult<P, W>
where
    P: PathServer<'s>,
{
    pub fn distance(&self) -> W {
        self.distance
    }

    pub fn path(&'s mut self) -> Vec<P::NodeInfo> {
        self.path_server.path()
    }
}

pub trait QueryServer<'s> {
    type P: PathServer<'s>;
    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>>;
}

pub trait TDQueryServer<'s, T: Copy, W> {
    type P: PathServer<'s>;
    fn query(&'s mut self, query: TDQuery<T>) -> Option<QueryResult<Self::P, W>>;
}

pub trait PathServer<'s> {
    type NodeInfo: 'static;
    fn path(&'s mut self) -> Vec<Self::NodeInfo>;
}
