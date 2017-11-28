use super::*;
use ::io;
use std::io::Result;
use std::path::Path;
use std::mem::swap;

#[derive(Debug, Clone)]
pub struct FirstOutGraph {
    // index of first edge of each node +1 entry in the end
    first_out: Vec<u32>,
    // the node ids to which each edge points
    head: Vec<NodeId>,
    // the weight of each edge
    weight: Vec<Weight>
}

impl FirstOutGraph {
    pub fn new(first_out: Vec<u32>, head: Vec<NodeId>, weight: Vec<Weight>) -> FirstOutGraph {
        assert_eq!(*first_out.first().unwrap(), 0);
        assert_eq!(*first_out.last().unwrap() as usize, head.len());
        assert_eq!(weight.len(), head.len());

        FirstOutGraph {
            first_out, head, weight
        }
    }

    pub fn from_adjancecy_lists(adjancecy_lists: Vec<Vec<Link>>) -> FirstOutGraph {
        // create first_out array by doing a prefix sum over the adjancecy list sizes
        let first_out = std::iter::once(0).chain(adjancecy_lists.iter().scan(0, |state, incoming_links| {
            *state = *state + incoming_links.len() as u32;
            Some(*state)
        })).collect();

        // append all adjancecy list and split the pairs into two seperate vectors
        let (head, weight) = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter().map(|Link { node, weight }| (node, weight) ) )
            .unzip();

        FirstOutGraph::new(first_out, head, weight)
    }

    pub fn neighbor_iter(&self, node: NodeId) -> std::iter::Map<std::iter::Zip<std::slice::Iter<NodeId>, std::slice::Iter<Weight>>, fn((&NodeId, &Weight))->Link> {
        let range = (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize);
        self.head[range.clone()].iter()
            .zip(self.weight[range].iter())
            .map( |(&neighbor, &weight)| Link { node: neighbor, weight: weight } )
    }

    pub fn neighbor_iter_mut(&mut self, node: NodeId) -> std::iter::Zip<std::slice::IterMut<NodeId>, std::slice::IterMut<Weight>> {
        let range = (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize);
        self.head[range.clone()].iter_mut().zip(self.weight[range].iter_mut())
    }

    pub fn edge_index(&self, from: NodeId, to: NodeId) -> Option<usize> {
        let first_out = self.first_out[from as usize] as usize;
        self.neighbor_iter(from).enumerate().find(|&(_, Link { node, .. })| node == to).map(|(i, _)| first_out + i )
    }

    pub fn reverse(&self) -> FirstOutGraph {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                reversed[neighbor as usize].push(Link { node, weight });
            }
        }
        FirstOutGraph::from_adjancecy_lists(reversed)
    }

    pub fn ch_split(self, node_ranks: &Vec<u32>) -> (FirstOutGraph, FirstOutGraph) {
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

        (FirstOutGraph::from_adjancecy_lists(up), FirstOutGraph::from_adjancecy_lists(down))
    }

    pub fn write_to_dir(&self, dir: &str) -> Result<()> {
        let path = Path::new(dir);
        let res1 = io::write_vector_to_file(path.join("first_out").to_str().unwrap(), &self.first_out);
        let res2 = io::write_vector_to_file(path.join("head").to_str().unwrap(), &self.head);
        let res3 = io::write_vector_to_file(path.join("weights").to_str().unwrap(), &self.weight);
        res1.and(res2).and(res3)
    }

    pub fn swap_weights(&mut self, mut new_weights: &mut Vec<Weight>) {
        assert!(new_weights.len() == self.weight.len());
        swap(&mut self.weight, &mut new_weights);
    }
}

impl DijkstrableGraph for FirstOutGraph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn for_each_neighbor(&self, node: NodeId, f: &mut FnMut(Link)) {
        for link in self.neighbor_iter(node) {
            f(link);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reversal() {
        let graph = FirstOutGraph::new(
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
        let expected = FirstOutGraph::new(
            vec![0,  1,     3,   4,     6,     8,8],
            vec![3,  0, 2,  0,   1, 2,  2, 3],
            vec![7,  1, 1,  10,  2, 3,  1, 2]);
        let reversed = graph.reverse();

        assert_eq!(reversed.first_out, expected.first_out);
        assert_eq!(reversed.head, expected.head);
        assert_eq!(reversed.weight, expected.weight);
    }
}
