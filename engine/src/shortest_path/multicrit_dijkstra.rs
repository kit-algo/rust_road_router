use std::fmt::Debug;
use std::collections::BinaryHeap;
use super::*;

use std::cmp::Ordering;
pub trait Label: Ord + Debug + Clone {
    fn node(&self) -> NodeId;
    fn pareto_cmp(&self, other: &Self) -> Option<Ordering>;
    fn parent(&self) -> (NodeId, usize);
    fn zero() -> Self;
}

pub trait Link: Debug {
    type LabelType: Label;
    fn relax(&self, label: &Self::LabelType, label_index: usize) -> Self::LabelType;
    fn head(&self) -> NodeId;
}

pub trait MCDGraph: Graph + Debug {
    type LinkType: Link;
    type Iter: Iterator<Item = Self::LinkType>;

    fn neighbor_iter(&self, node: NodeId) -> Self::Iter;
}

#[derive(Debug)]
pub struct MultiCritDijkstra<Graph: MCDGraph> {
    graph: Graph,
    labels: Vec<Vec<<<Graph as MCDGraph>::LinkType as Link>::LabelType>>,
    queue: BinaryHeap<<<Graph as MCDGraph>::LinkType as Link>::LabelType>,
}

impl<Graph: MCDGraph> MultiCritDijkstra<Graph> {
    pub fn new(graph: Graph) -> MultiCritDijkstra<Graph> {
        let n = graph.num_nodes();

        MultiCritDijkstra {
            graph,
            labels: vec![Vec::new(); n],
            queue: BinaryHeap::new(),
        }
    }

    pub fn query(&mut self, query: Query) -> &Vec<<<Graph as MCDGraph>::LinkType as Link>::LabelType> {
        self.queue.clear();
        for node_labels in &mut self.labels {
            node_labels.clear();
        }
        let zero = <<Graph as MCDGraph>::LinkType as Link>::LabelType::zero();

        self.labels[query.from as usize].push(zero.clone());
        self.queue.push(zero);

        while let Some(label) = self.queue.pop() {
            if !self.labels[label.node() as usize].iter().any(|other_label| label.pareto_cmp(other_label) == Some(Ordering::Greater)) {
                let label_index = self.labels[label.node() as usize].len();

                for link in self.graph.neighbor_iter(label.node()) {
                    if link.head() != label.parent().0 { // Hopping reduction - don't relax back to parent
                        let new_label = link.relax(&label, label_index);

                        if !self.labels[query.to as usize].iter().any(|other_label| new_label.pareto_cmp(other_label) == Some(Ordering::Greater)) && // Target Pruning - forget label if dominate by label at target
                            !self.labels[new_label.node() as usize].iter().any(|other_label| new_label.pareto_cmp(other_label) == Some(Ordering::Greater)) {
                            self.queue.push(new_label);
                        }
                    }
                }

                self.labels[label.node() as usize].push(label);
            }
        }

        &self.labels[query.to as usize]
    }
}
