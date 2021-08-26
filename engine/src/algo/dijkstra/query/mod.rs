//! Query Server wrappers for the different dijkstra variants

use super::*;

pub mod bidirectional_dijkstra;
pub mod dijkstra;
pub mod floating_td_dijkstra;
pub mod td_dijkstra;

pub mod disconnected_targets {
    use super::*;
    use crate::datastr::graph::time_dependent::Timestamp;
    use crate::datastr::rank_select_map::*;

    pub struct CatchDisconnectedTarget<S> {
        server: S,
        reversed: UnweightedOwnedGraph,
        visited: FastClearBitVec,
    }

    impl<S> CatchDisconnectedTarget<S> {
        pub fn new<G>(server: S, graph: &G) -> Self
        where
            UnweightedOwnedGraph: BuildReversed<G>,
            G: Graph,
        {
            Self {
                server,
                reversed: UnweightedOwnedGraph::reversed(&graph),
                visited: FastClearBitVec::new(graph.num_nodes()),
            }
        }

        fn dfs(graph: &UnweightedOwnedGraph, node: NodeId, visited: &mut FastClearBitVec, visit: &mut impl FnMut(NodeId) -> bool) {
            if visited.get(node as usize) {
                return;
            }
            visited.set(node as usize);
            if visit(node) {
                return;
            }
            for NodeIdT(head) in LinkIterable::<NodeIdT>::link_iter(graph, node) {
                Self::dfs(graph, head, visited, visit);
            }
        }

        pub fn check_target(&mut self, source: NodeId, target: NodeId) -> bool {
            let mut counter = 0;
            let mut reachable = false;
            self.visited.clear();
            Self::dfs(&self.reversed, target, &mut self.visited, &mut |node| {
                if node == source {
                    reachable = true;
                    return false;
                }
                if counter < 100 {
                    counter += 1;
                    false
                } else {
                    reachable = true;
                    true
                }
            });
            return reachable;
        }
    }

    impl<S> TDQueryServer<Timestamp, Weight> for CatchDisconnectedTarget<S>
    where
        S: TDQueryServer<Timestamp, Weight>,
    {
        type P<'s>
        where
            Self: 's,
        = Option<S::P<'s>>;

        fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
            if self.check_target(query.from(), query.to()) {
                let (dist, path_server) = self.server.td_query(query).decompose();
                QueryResult::new(dist, Some(path_server))
            } else {
                QueryResult::new(None, None)
            }
        }
    }

    impl<S> QueryServer for CatchDisconnectedTarget<S>
    where
        S: QueryServer,
    {
        type P<'s>
        where
            Self: 's,
        = Option<S::P<'s>>;

        fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
            if self.check_target(query.from(), query.to()) {
                let (dist, path_server) = self.server.query(query).decompose();
                QueryResult::new(dist, Some(path_server))
            } else {
                QueryResult::new(None, None)
            }
        }
    }

    impl<P: PathServer> PathServer for Option<P> {
        type NodeInfo = P::NodeInfo;
        type EdgeInfo = P::EdgeInfo;

        fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
            self.as_mut().map_or(Vec::new(), P::reconstruct_node_path)
        }
        fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
            self.as_mut().map_or(Vec::new(), P::reconstruct_edge_path)
        }
    }
}
