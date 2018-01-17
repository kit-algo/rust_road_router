use super::*;
use as_slice::AsSlice;
use ::io;
use std::io::Result;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<u32>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    // index of first edge of each node +1 entry in the end
    first_out: FirstOutContainer,
    // the node ids to which each edge points
    head: HeadContainer,
    // the weight of each edge
    weight: WeightContainer,
}

pub type OwnedGraph = FirstOutGraph<Vec<u32>, Vec<NodeId>, Vec<Weight>>;

impl<FirstOutContainer, HeadContainer, WeightContainer> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<u32>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn first_out(&self) -> &[u32] { self.first_out.as_slice() }
    fn head(&self) -> &[u32] { self.head.as_slice() }
    fn weight(&self) -> &[u32] { self.weight.as_slice() }

    pub fn new(first_out: Vec<u32>, head: Vec<NodeId>, weight: Vec<Weight>) -> OwnedGraph {
        assert_eq!(*first_out.first().unwrap(), 0);
        assert_eq!(*first_out.last().unwrap() as usize, head.len());
        assert_eq!(weight.len(), head.len());

        FirstOutGraph { first_out, head, weight }
    }

    pub fn write_to_dir(&self, dir: &str) -> Result<()> {
        let path = Path::new(dir);
        let res1 = io::write_vector_to_file(path.join("first_out").to_str().unwrap(), self.first_out());
        let res2 = io::write_vector_to_file(path.join("head").to_str().unwrap(), self.head());
        let res3 = io::write_vector_to_file(path.join("weights").to_str().unwrap(), self.weight());
        res1.and(res2).and(res3)
    }
}

impl OwnedGraph {
    pub fn from_adjancecy_lists(adjancecy_lists: Vec<Vec<Link>>) -> OwnedGraph {
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

        OwnedGraph::new(first_out, head, weight)
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> Graph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<u32>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn num_nodes(&self) -> usize {
        self.first_out().len() - 1
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> LinkIterGraph<'a> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<u32>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    type Iter = std::iter::Map<std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::Iter<'a, Weight>>, fn((&NodeId, &Weight))->Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = (self.first_out()[node as usize] as usize)..(self.first_out()[(node + 1) as usize] as usize);
        self.head()[range.clone()].iter()
            .zip(self.weight()[range].iter())
            .map( |(&neighbor, &weight)| Link { node: neighbor, weight: weight } )
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> RandomLinkAccessGraph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<u32>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<usize> {
        let first_out = self.first_out()[from as usize] as usize;
        self.neighbor_iter(from).enumerate().find(|&(_, Link { node, .. })| node == to).map(|(i, _)| first_out + i )
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
