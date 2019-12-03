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

#[derive(Debug, Clone, Copy)]
pub struct Query {
    pub from: NodeId,
    pub to: NodeId,
}

#[derive(Debug, Clone, Copy)]
pub struct TDQuery<T: Copy> {
    pub from: NodeId,
    pub to: NodeId,
    pub departure: T,
}

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
    type NodeInfo;
    fn path(&'s mut self) -> Vec<Self::NodeInfo>;
}
