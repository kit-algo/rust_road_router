pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

mod shortest_path;

#[derive(Debug)]
pub struct Graph {
    // index of first edge of each node +1 entry in the end
    first_out: Vec<u32>,
    // the node ids to which each edge points
    head: Vec<NodeId>,
    // the weight of each edge
    weight: Vec<Weight>
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use shortest_path::shortest_path;

    #[test]
    fn it_works() {
        // This is the directed graph we're going to use.
        // The node numbers correspond to the different states,
        // and the edge weights symbolize the cost of moving
        // from one node to another.
        // Note that the edges are one-way.
        //
        //                  7
        //          +-----------------+
        //          |                 |
        //          v   1        2    |  2
        //          0 -----> 1 -----> 3 ---> 4
        //          |        ^        ^      ^
        //          |        | 1      |      |
        //          |        |        | 3    | 1
        //          +------> 2 -------+      |
        //           10      |               |
        //                   +---------------+
        //
        let graph = Graph::new(
            vec![0,      2,  3,        6,    8, 8, 8],
            vec![2,  1,  3,  1, 3, 4,  0, 4],
            vec![10, 1,  2,  1, 3, 1,  7, 2]);
        // The graph is represented as an adjacency list where each index,
        // corresponding to a node value, has a list of outgoing edges.
        // Chosen for its efficiency.
        // let graph = vec![
        //     // Node 0
        //     vec![Edge { node: 2, cost: 10 },
        //          Edge { node: 1, cost: 1 }],
        //     // Node 1
        //     vec![Edge { node: 3, cost: 2 }],
        //     // Node 2
        //     vec![Edge { node: 1, cost: 1 },
        //          Edge { node: 3, cost: 3 },
        //          Edge { node: 4, cost: 1 }],
        //     // Node 3
        //     vec![Edge { node: 0, cost: 7 },
        //          Edge { node: 4, cost: 2 }],
        //     // Node 4
        //     vec![]];

        assert_eq!(shortest_path(&graph, 0, 1), Some(1));
        assert_eq!(shortest_path(&graph, 0, 3), Some(3));
        assert_eq!(shortest_path(&graph, 3, 0), Some(7));
        assert_eq!(shortest_path(&graph, 0, 4), Some(5));
        assert_eq!(shortest_path(&graph, 4, 0), None);
    }
}


