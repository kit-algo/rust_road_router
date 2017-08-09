use std;

pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug, Clone)]
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
    pub weight: Weight
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
            .map( |(&neighbor, &weight)| Link { node: neighbor, weight: weight } )
    }

    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    pub fn reverse(&self) -> Graph {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                reversed[neighbor as usize].push(Link { node, weight });
            }
        }
        Graph::adjancecy_lists_to_graph(reversed)
    }

    pub fn ch_split(self, node_ranks: &Vec<u32>) -> (Graph, Graph) {
        let mut up: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();
        let mut down: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                if node_ranks[node as usize] < node_ranks[neighbor as usize] {
                    up[node as usize].push(Link { node: neighbor, weight });
                } else {
                    down[neighbor as usize].push(Link { node, weight });
                }
            }
        }

        (Graph::adjancecy_lists_to_graph(up), Graph::adjancecy_lists_to_graph(down))
    }

    fn adjancecy_lists_to_graph(adjancecy_lists: Vec<Vec<Link>>) -> Graph {
        // create first_out array for reversed by doing a prefix sum over the adjancecy list sizes
        let first_out = std::iter::once(0).chain(adjancecy_lists.iter().scan(0, |state, incoming_links| {
            *state = *state + incoming_links.len() as u32;
            Some(*state)
        })).collect();

        // append all adjancecy list and split the pairs into two seperate vectors
        let (head, weight) = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter().map(|Link { node, weight }| (node, weight) ) )
            .unzip();

        Graph::new(first_out, head, weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reversal() {
        let graph = Graph::new(
            vec![0,      2,  3,        6,    8, 8, 8],
            vec![2,  1,  3,  1, 3, 4,  0, 4],
            vec![10, 1,  2,  1, 3, 1,  7, 2]);

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
        let expected = Graph::new(
            vec![0,  1,     3,   4,     6,     8,8],
            vec![3,  0, 2,  0,   1, 2,  2, 3],
            vec![7,  1, 1,  10,  2, 3,  1, 2]);
        let reversed = graph.reverse();

        assert_eq!(reversed.first_out, expected.first_out);
        assert_eq!(reversed.head, expected.head);
        assert_eq!(reversed.weight, expected.weight);
    }
}
