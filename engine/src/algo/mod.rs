//! Building blocks for fast routing algorithms.

use crate::datastr::graph::*;

use self::dijkstra::{stepped_dijkstra::SteppedDijkstra, QueryProgress, State};

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
/// Contains a distance and allows fetching the actual path.
/// Since queries usually modify the state of the internal algorithm data structures,
/// it is usually impossible to retrieve a path for an older query result once a new query was performed.
/// This type uses rusts ownership rules to enforce this behaviour through the type signatures.
/// This works fine:
///
/// ```
/// # use bmw_routing_engine::algo::*;
/// fn fine(mut server: impl for<'s> QueryServer<'s>) {
///     let mut result = server.query(Query { from: 0, to: 1 }).unwrap();
///     dbg!(result.distance());
///     dbg!(result.path().len());
///     std::mem::drop(result); // necessary because of higher ranked trait bound
///     let mut result2 = server.query(Query { from: 1, to: 0 }).unwrap();
///     dbg!(result2.distance());
///     dbg!(result2.path().len());
/// }
/// ```
///
/// This will fail to compile:
///
/// ```compile_fail
/// # use bmw_routing_engine::algo::*;
/// fn fine(mut server: impl for<'s> QueryServer<'s>) {
///     let mut result = server.query(Query { from: 0, to: 1 }).unwrap();
///     dbg!(result.distance());
///     let mut result2 = server.query(Query { from: 1, to: 0 }).unwrap();
///     dbg!(result.path().len());
/// }
/// ```
#[derive(Debug)]
pub struct QueryResult<'s, P, W> {
    // just the plain distance
    // the infinity case is ignored here, in that case, there will be no query result at all
    distance: W,
    // Reference to some object - usually a proxy around the query server to lazily retrieve the path
    // Usually this will borrow the server, but the type checker does not know this in a generic context
    path_server: P,
    // To tell the type checker, that we have a reference of the server we use this phantomdata
    // Actually this is not strictly necessary - it just makes the generic context behave the same
    // As currently all concrete implementations.
    // If we wanted to relax the requirements to only the query types which actually needed to borrow
    // the query state for path retrieval, we could drop this phantom.
    phantom: std::marker::PhantomData<&'s ()>,
}

impl<'s, P, W: Copy> QueryResult<'s, P, W>
where
    P: PathServer,
{
    fn new(distance: W, path_server: P) -> Self {
        Self {
            distance,
            path_server,
            phantom: std::marker::PhantomData {},
        }
    }

    /// Retrieve shortest distance of a query
    pub fn distance(&self) -> W {
        self.distance
    }

    /// Retrieve shortest path (usually lazily) for a query
    pub fn path(&mut self) -> Vec<P::NodeInfo> {
        self.path_server.path()
    }

    /// Get reference to object which allows to access additional query specific data
    pub fn data(&mut self) -> &mut P {
        &mut self.path_server
    }
}

/// Trait for query algorithm servers.
/// The lifetime parameter is necessary, so the PathServer type can have a lifetime parameter.
pub trait QueryServer<'s> {
    /// Just for internal use. Type of the object that can retrieve the actual shortest path.
    type P: PathServer;
    /// Calculate the shortest distance from a given source to target.
    /// Will return None if source and target are not connected.
    fn query(&'s mut self, query: Query) -> Option<QueryResult<'s, Self::P, Weight>>;
}

/// Trait for time-dependent query algorithm servers.
/// The lifetime parameter is necessary, so the PathServer type can have a lifetime parameter.
pub trait TDQueryServer<'s, T: Copy, W> {
    /// Just for internal use. Type of the object that can retrieve the actual shortest path.
    type P: PathServer;
    /// Calculate the shortest distance from a given source to target.
    /// Will return None if source and target are not connected.
    fn query(&'s mut self, query: TDQuery<T>) -> Option<QueryResult<'s, Self::P, W>>;
}

/// Just for internal use.
/// Trait for path retrievers.
pub trait PathServer {
    /// Information for each node in the path.
    type NodeInfo;
    /// Fetch the shortest path.
    fn path(&mut self) -> Vec<Self::NodeInfo>;
}
