pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug)]
pub struct Graph {
    first_out: Vec<u32>,
    head: Vec<NodeId>,
    weight: Vec<Weight>
}

pub struct Link {
    pub node: NodeId,
    pub cost: Weight
}

impl Graph {
    pub fn new(first_out: Vec<u32>, head: Vec<NodeId>, weight: Vec<Weight>) -> Graph {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}


