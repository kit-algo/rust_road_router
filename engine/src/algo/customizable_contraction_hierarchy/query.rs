//! CCH query based on elimination tree

use super::*;
pub mod stepped_elimination_tree;
use stepped_elimination_tree::SteppedEliminationTree;

#[derive(Debug)]
pub struct Server<'a, CCH> {
    forward: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    backward: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    cch: &'a CCH,
    tentative_distance: Weight,
    meeting_node: NodeId,
}

impl<'a, CCH: CCHT> Server<'a, CCH> {
    pub fn new(customized: Customized<'a, CCH>) -> Self {
        let cch = customized.cch;
        let (forward, backward) = customized.into_ch_graphs();
        let forward = SteppedEliminationTree::new(forward, cch.elimination_tree());
        let backward = SteppedEliminationTree::new(backward, cch.elimination_tree());

        Server {
            forward,
            backward,
            cch,
            tentative_distance: INFINITY,
            meeting_node: 0,
        }
    }

    // Update the metric using a new customization result
    pub fn update(&mut self, mut customized: Customized<'a, CCH>) {
        self.forward.graph_mut().swap_weights(&mut customized.upward);
        self.backward.graph_mut().swap_weights(&mut customized.downward);
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.cch.node_order().rank(from);
        let to = self.cch.node_order().rank(to);

        // initialize
        self.tentative_distance = INFINITY;
        self.meeting_node = 0;
        self.forward.initialize_query(from);
        self.backward.initialize_query(to);

        // walk up forward elimination tree
        while self.forward.next().is_some() {
            self.forward.next_step();
        }

        // walk up backward elimination tree while updating tentative distances
        while let QueryProgress::Settled(State { key, node }) = self.backward.next_step() {
            if key + self.forward.tentative_distance(node) < self.tentative_distance {
                self.tentative_distance = key + self.forward.tentative_distance(node);
                self.meeting_node = node;
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    fn path(&mut self) -> Vec<NodeId> {
        // unpack shortcuts so that parant pointers already point along the completely unpacked path
        self.forward.unpack_path(self.meeting_node, true, self.cch, self.backward.graph().weight());
        self.backward.unpack_path(self.meeting_node, true, self.cch, self.forward.graph().weight());

        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != self.forward.origin() {
            path.push(self.forward.predecessor(*path.last().unwrap()));
        }

        path.reverse();

        while *path.last().unwrap() != self.backward.origin() {
            path.push(self.backward.predecessor(*path.last().unwrap()));
        }

        for node in &mut path {
            *node = self.cch.node_order().node(*node);
        }

        path
    }
}

pub struct PathServerWrapper<'s, 'a, CCH>(&'s mut Server<'a, CCH>);

impl<'s, 'a, CCH: CCHT> PathServer for PathServerWrapper<'s, 'a, CCH> {
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<'a, CCH: CCHT> QueryServer for Server<'a, CCH> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, 'a, CCH>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query.from, query.to), PathServerWrapper(self))
    }
}
