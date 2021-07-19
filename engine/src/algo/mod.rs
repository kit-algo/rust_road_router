//! Building blocks for fast routing algorithms.

use crate::datastr::{graph::*, node_order::NodeOrder};

use self::dijkstra::{QueryProgress, State};

pub mod a_star;
pub mod alt;
pub mod catchup;
pub mod ch_potentials;
pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod dijkstra;
pub mod hl;
pub mod time_dependent_sampling;
pub mod topocore;

pub trait GenQuery<Label> {
    fn new(from: NodeId, to: NodeId, initial_state: Label) -> Self;
    fn from(&self) -> NodeId;
    fn to(&self) -> NodeId;
    fn initial_state(&self) -> Label;
    fn permutate(&mut self, order: &NodeOrder);
}

/// Simply a source-target pair
#[derive(Debug, Clone, Copy)]
pub struct Query {
    pub from: NodeId,
    pub to: NodeId,
}

impl GenQuery<Weight> for Query {
    fn new(from: NodeId, to: NodeId, _initial_state: Weight) -> Self {
        Query { from, to }
    }

    fn from(&self) -> NodeId {
        self.from
    }
    fn to(&self) -> NodeId {
        self.to
    }
    fn initial_state(&self) -> Weight {
        0
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.from = order.rank(self.from);
        self.to = order.rank(self.to);
    }
}

/// A source-target pair with a departure time.
/// Genric over the timestamp type, so we can support both integer and float weights
#[derive(Debug, Clone, Copy)]
pub struct TDQuery<T: Copy> {
    pub from: NodeId,
    pub to: NodeId,
    pub departure: T,
}

impl<T: Copy> GenQuery<T> for TDQuery<T> {
    fn new(from: NodeId, to: NodeId, initial_state: T) -> Self {
        TDQuery {
            from,
            to,
            departure: initial_state,
        }
    }
    fn from(&self) -> NodeId {
        self.from
    }
    fn to(&self) -> NodeId {
        self.to
    }
    fn initial_state(&self) -> T {
        self.departure
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.from = order.rank(self.from);
        self.to = order.rank(self.to);
    }
}

/// Generic container for query results.
/// Contains a distance and allows fetching the actual path.
/// Since queries usually modify the state of the internal algorithm data structures,
/// it is usually impossible to retrieve a path for an older query result once a new query was performed.
/// This type uses rusts ownership rules to enforce this behaviour through the type signatures.
/// This works fine:
///
/// ```
/// # use rust_road_router::algo::*;
/// fn fine(mut server: impl QueryServer) {
///     let mut result = server.query(Query { from: 0, to: 1 }).found().unwrap();
///     dbg!(result.distance());
///     dbg!(result.path().len());
///     std::mem::drop(result);
///     let mut result2 = server.query(Query { from: 1, to: 0 }).found().unwrap();
///     dbg!(result2.distance());
///     dbg!(result2.path().len());
/// }
/// ```
///
/// This will fail to compile:
///
/// ```compile_fail
/// # use rust_road_router::algo::*;
/// fn fine(mut server: impl for QueryServer) {
///     let mut result = server.query(Query { from: 0, to: 1 }).found().unwrap();
///     dbg!(result.distance());
///     let mut result2 = server.query(Query { from: 1, to: 0 }).found().unwrap();
///     dbg!(result.path().len());
/// }
/// ```
#[derive(Debug)]
pub struct QueryResult<P, W> {
    // just the plain distance
    // the infinity case is ignored here, in that case, there will be no query result at all
    distance: Option<W>,
    // Reference to some object - usually a proxy around the query server to lazily retrieve the path
    // Usually this will borrow the server, but the type checker does not know this in a generic context
    path_server: P,
}

impl<P, W: Copy> QueryResult<P, W>
where
    P: PathServer,
{
    fn new(distance: Option<W>, path_server: P) -> Self {
        Self { distance, path_server }
    }

    /// Retrieve shortest distance of a query
    pub fn distance(&self) -> Option<W> {
        self.distance
    }

    /// Retrieve shortest path (usually lazily) for a query
    pub fn node_path(&mut self) -> Option<Vec<P::NodeInfo>> {
        if self.distance.is_some() {
            Some(self.path_server.reconstruct_node_path())
        } else {
            None
        }
    }

    /// Retrieve shortest path as edge list (usually lazily) for a query
    pub fn edge_path(&mut self) -> Option<Vec<P::EdgeInfo>> {
        if self.distance.is_some() {
            Some(self.path_server.reconstruct_edge_path())
        } else {
            None
        }
    }

    /// Get reference to object which allows to access additional query specific data
    pub fn data(&mut self) -> &mut P {
        &mut self.path_server
    }

    pub fn found(self) -> Option<ConnectedQueryResult<P, W>> {
        if self.distance.is_some() {
            Some(ConnectedQueryResult(self))
        } else {
            None
        }
    }
}

pub struct ConnectedQueryResult<P, W>(QueryResult<P, W>);

impl<P, W: Copy> ConnectedQueryResult<P, W>
where
    P: PathServer,
{
    /// Retrieve shortest distance of a query
    pub fn distance(&self) -> W {
        self.0.distance.unwrap()
    }

    /// Retrieve shortest path (usually lazily) for a query
    pub fn node_path(&mut self) -> Vec<P::NodeInfo> {
        self.0.node_path().unwrap()
    }

    /// Retrieve shortest path as edge list (usually lazily) for a query
    pub fn edge_path(&mut self) -> Vec<P::EdgeInfo> {
        self.0.edge_path().unwrap()
    }

    /// Get reference to object which allows to access additional query specific data
    pub fn data(&mut self) -> &mut P {
        &mut self.0.path_server
    }
}

/// Trait for query algorithm servers.
pub trait QueryServer {
    /// Just for internal use. Type of the object that can retrieve the actual shortest path.
    type P<'s>: PathServer
    where
        Self: 's;
    /// Calculate the shortest distance from a given source to target.
    /// Will return None if source and target are not connected.
    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight>;
}

/// Trait for time-dependent query algorithm servers.
/// The lifetime parameter is necessary, so the PathServer type can have a lifetime parameter.
pub trait TDQueryServer<T: Copy, W> {
    /// Just for internal use. Type of the object that can retrieve the actual shortest path.
    type P<'s>: PathServer
    where
        Self: 's;
    /// Calculate the shortest distance from a given source to target.
    /// Will return None if source and target are not connected.
    fn td_query(&mut self, query: TDQuery<T>) -> QueryResult<Self::P<'_>, W>;
}

/// Just for internal use.
/// Trait for path retrievers.
pub trait PathServer {
    /// Information for each node in the path.
    type NodeInfo;
    /// Information for each edge in the path.
    type EdgeInfo;
    /// Fetch the shortest path.
    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo>;
    /// Fetch the shortest path as edges.
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo>;
}
