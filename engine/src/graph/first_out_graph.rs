use super::*;
use crate::as_slice::AsSlice;
use crate::as_mut_slice::AsMutSlice;
use crate::io::*;
use std::io::Result;
use std::path::Path;
use std::mem::swap;


#[derive(Debug, Clone)]
pub struct FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
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

impl<FirstOutContainer, HeadContainer, WeightContainer> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn first_out(&self) -> &[EdgeId] { self.first_out.as_slice() }
    fn head(&self) -> &[NodeId] { self.head.as_slice() }
    fn weight(&self) -> &[Weight] { self.weight.as_slice() }

    pub fn new(first_out: FirstOutContainer, head: HeadContainer, weight: WeightContainer) -> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> {
        assert!(first_out.as_slice().len() < <NodeId>::max_value() as usize);
        assert!(head.as_slice().len() < <EdgeId>::max_value() as usize);
        assert_eq!(*first_out.as_slice().first().unwrap(), 0);
        assert_eq!(*first_out.as_slice().last().unwrap() as usize, head.as_slice().len());
        assert_eq!(weight.as_slice().len(), head.as_slice().len());

        FirstOutGraph { first_out, head, weight }
    }

    pub fn degree(&self, node: NodeId) -> usize {
        let range = self.neighbor_edge_indices_usize(node);
        range.end - range.start
    }

    pub fn write_to_dir(&self, dir: &str) -> Result<()> {
        let path = Path::new(dir);
        let res1 = self.first_out().write_to(path.join("first_out").to_str().unwrap());
        let res2 = self.head().write_to(path.join("head").to_str().unwrap());
        let res3 = self.weight().write_to(path.join("weights").to_str().unwrap());
        res1.and(res2).and(res3)
    }

    pub fn decompose(self) -> (FirstOutContainer, HeadContainer, WeightContainer) {
        (self.first_out, self.head, self.weight)
    }
}

pub type OwnedGraph = FirstOutGraph<Vec<EdgeId>, Vec<NodeId>, Vec<Weight>>;

impl OwnedGraph {
    pub fn from_adjancecy_lists(adjancecy_lists: Vec<Vec<Link>>) -> OwnedGraph {
        // create first_out array by doing a prefix sum over the adjancecy list sizes
        let first_out = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.len() as EdgeId);
            degrees_to_first_out(degrees).collect()
        };

        // append all adjancecy list and split the pairs into two seperate vectors
        let (head, weight) = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter().map(|Link { node, weight }| (node, weight) ) )
            .unzip();

        OwnedGraph::new(first_out, head, weight)
    }
}

impl<FirstOutContainer, HeadContainer> FirstOutGraph<FirstOutContainer, HeadContainer, Vec<Weight>> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    pub fn swap_weights(&mut self, mut new_weights: &mut Vec<Weight>) {
        assert!(new_weights.len() == self.weight.len());
        swap(&mut self.weight, &mut new_weights);
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> Graph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn num_nodes(&self) -> usize {
        self.first_out().len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head().len()
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> LinkIterGraph<'a> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    type Iter = std::iter::Map<std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::Iter<'a, Weight>>, fn((&NodeId, &Weight))->Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head()[range.clone()].iter()
            .zip(self.weight()[range].iter())
            .map( |(&neighbor, &weight)| Link { node: neighbor, weight } )
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> MutWeightLinkIterGraph<'a> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight> + AsMutSlice<Weight>,
{
    type Iter = std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::IterMut<'a, Weight>>;

    fn mut_weight_link_iter(&'a mut self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head.as_slice()[range.clone()].iter().zip(self.weight.as_mut_slice()[range].iter_mut())
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> RandomLinkAccessGraph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn link(&self, edge_id: EdgeId) -> Link {
        Link { node: self.head()[edge_id as usize], weight: self.weight()[edge_id as usize] }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out()[from as usize] as usize;
        self.neighbor_iter(from).enumerate().find(|&(_, Link { node, .. })| node == to).map(|(i, _)| (first_out + i) as EdgeId )
    }

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out()[node as usize] as EdgeId)..(self.first_out()[(node + 1) as usize] as EdgeId)
    }
}

pub fn degrees_to_first_out<I: Iterator<Item = EdgeId>>(degrees: I) -> impl Iterator<Item = EdgeId> {
    std::iter::once(0).chain(degrees.scan(0, |state, degree| {
        *state += degree as EdgeId;
        Some(*state)
    }))
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
