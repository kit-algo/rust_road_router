use super::*;
use crate::datastr::rank_select_map::FastClearBitVec;

pub struct DijkstraNearestNeighbor<Graph = OwnedGraph> {
    graph: Graph,
    dijkstra: DijkstraData<Weight, ()>,
    targets: FastClearBitVec,
}

impl<G: LinkIterGraph> DijkstraNearestNeighbor<G> {
    pub fn new(graph: G) -> Self {
        Self {
            dijkstra: DijkstraData::new(graph.num_nodes()),
            targets: FastClearBitVec::new(graph.num_nodes()),
            graph,
        }
    }

    pub fn query(&mut self, source: NodeId, targets: &[NodeId], k: usize) -> Vec<(Weight, NodeId)> {
        self.targets.clear();
        for &t in targets {
            self.targets.set(t as usize);
        }

        let mut ops = DefaultOps();
        let mut run = DijkstraRun::query(&self.graph, &mut self.dijkstra, &mut ops, DijkstraInit::from(source));
        let mut closest = Vec::with_capacity(k);
        while let Some(node) = run.next() {
            if self.targets.get(node as usize) {
                closest.push((*run.tentative_distance(node), node));
            }
            if closest.len() >= k {
                break;
            }
        }

        closest
    }
}
