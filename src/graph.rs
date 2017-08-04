use std;

pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug)]
pub struct Graph {
    // index of first edge of each node +1 entry in the end
    first_out: Vec<u32>,
    // the node ids to which each edge points
    head: Vec<NodeId>,
    // the weight of each edge
    weight: Vec<Weight>
}

#[derive(Debug)]
pub struct Link {
    pub node: NodeId,
    pub cost: Weight
}

impl Graph {
    pub fn new(first_out: Vec<u32>, head: Vec<NodeId>, weight: Vec<Weight>) -> Graph {
        assert_eq!(*first_out.first().unwrap(), 0);
        assert_eq!(*first_out.last().unwrap() as usize, head.len());
        assert_eq!(weight.len(), head.len());

        Graph {
            first_out, head, weight
        }
    }

    pub fn neighbor_iter(&self, node: NodeId) -> std::iter::Map<std::iter::Zip<std::slice::Iter<NodeId>, std::slice::Iter<Weight>>, fn((&NodeId, &Weight))->Link> {
        let range = (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize);
        self.head[range.clone()].iter()
            .zip(self.weight[range].iter())
            .map( |(&neighbor, &weight)| Link { node: neighbor, cost: weight } )
    }

    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }
}
