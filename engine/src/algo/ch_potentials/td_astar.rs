use super::dijkstra::td_stepped_dijkstra::{QueryProgress, *};
use super::*;
use crate::algo::customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *};
use crate::datastr::graph::time_dependent::*;
use crate::datastr::timestamped_vector::TimestampedVector;
use crate::util::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct Server<'a> {
    dijkstra: TDSteppedDijkstra,
    backward: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    upward: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    cch: &'a CCH,
}

impl Server<'_> {
    pub fn new(graph: TDGraph, cch: &CCH) -> Server {
        let metric = (0..graph.num_arcs() as EdgeId)
            .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
            .collect::<Vec<Weight>>();

        let (upward, downward) = customize(cch, &FirstOutGraph::new(graph.first_out(), graph.head(), metric));
        let backward = SteppedEliminationTree::new(downward, cch.elimination_tree());

        Server {
            dijkstra: TDSteppedDijkstra::new(graph),
            backward,
            upward,
            cch,
            potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
        }
    }

    fn potential(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        upward: &FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
        backward: &SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        node: NodeId,
    ) -> Weight {
        if let Some(pot) = potentials[node as usize].value() {
            return pot;
        }

        let min_by_up = upward
            .neighbor_iter(node)
            .map(|edge| edge.weight + Self::potential(potentials, upward, backward, edge.node))
            .min()
            .unwrap_or(INFINITY);

        potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(backward.tentative_distance(node), min_by_up)));
        potentials[node as usize].value().unwrap()
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<Weight> {
        self.potentials.reset();
        self.backward.initialize_query(self.cch.node_order().rank(to));
        while self.backward.next().is_some() {
            self.backward.next_step();
        }

        self.dijkstra.initialize_query(TDQuery { from, to, departure_time });

        let dijkstra = &mut self.dijkstra;
        let potentials = &mut self.potentials;
        let upward = &self.upward;
        let backward = &self.backward;
        let cch = self.cch;

        loop {
            match dijkstra.next_step(|_| true, |node| Self::potential(potentials, upward, backward, cch.node_order().rank(node))) {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result,
            }
        }
    }
}
