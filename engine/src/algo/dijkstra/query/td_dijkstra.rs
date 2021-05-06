use super::*;
use crate::algo::dijkstra::generic_dijkstra::*;
use crate::datastr::graph::time_dependent::*;

#[derive(Debug, Clone, Copy)]
pub struct TDDijkstraOps();

impl DijkstraOps<TDGraph> for TDDijkstraOps {
    type Label = Weight;
    type LinkResult = Weight;
    type Arc = (NodeId, EdgeId);

    #[inline(always)]
    fn link(&mut self, graph: &TDGraph, label: &Weight, link: &Self::Arc) -> Self::LinkResult {
        label + graph.travel_time_function(link.1).eval(*label)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
}

impl Default for TDDijkstraOps {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LiveTDDijkstraOps();

impl DijkstraOps<LiveTDGraph> for LiveTDDijkstraOps {
    type Label = Weight;
    type LinkResult = Weight;
    type Arc = (NodeId, EdgeId);

    #[inline(always)]
    fn link(&mut self, graph: &LiveTDGraph, label: &Weight, link: &Self::Arc) -> Self::LinkResult {
        label + graph.eval(link.1, *label)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
}

impl Default for LiveTDDijkstraOps {
    fn default() -> Self {
        Self {}
    }
}
