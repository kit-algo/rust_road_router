//! Goto static graph representation for route planning algorithms.
//!
//! Nodes and edges can be identified by a unique id, going from `0` to `n-1` and `m-1` respectively, where `n` is the number of nodes and `m` the number of directed arcs.
//! We store the graph as an adjacency array using three collections: `first_out`, `head` and `weight`.
//! `head` and `weight` have each `m` elements.
//! `first_out` has `n+1` elements.
//! The first element of `first_out` is always 0 and the last one `m`.
//! `first_out[x]` contains the id of the first edge that is an outgoing edge of node `x`.
//! Thus, `head[first_out[x]..first_out[x+1]]` contains all neighbors of `x`.

use super::*;
use crate::as_mut_slice::AsMutSlice;
use crate::as_slice::AsSlice;
use crate::io::*;
use std::mem::swap;

/// Container struct for the three collections of a graph.
/// Genric over the types of the three data collections.
/// Anything that can be dereferenced to a slice works.
/// Both owned (`Vec<T>`, `Box<[T]>`) and shared (`Rc<[T]>`, `Arc<[T])>`) or borrowed (slices) data is possible.
#[derive(Debug, Clone)]
pub struct FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
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

impl<FirstOutContainer, HeadContainer, WeightContainer> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    /// Borrow a slice of the first_out data
    pub fn first_out(&self) -> &[EdgeId] {
        self.first_out.as_slice()
    }
    /// Borrow a slice of the head data
    pub fn head(&self) -> &[NodeId] {
        self.head.as_slice()
    }
    /// Borrow a slice of the weight data
    pub fn weight(&self) -> &[Weight] {
        self.weight.as_slice()
    }

    /// Create a new `FirstOutGraph` from the three containers.
    pub fn new(first_out: FirstOutContainer, head: HeadContainer, weight: WeightContainer) -> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer> {
        assert!(first_out.as_slice().len() < <NodeId>::max_value() as usize);
        assert!(head.as_slice().len() < <EdgeId>::max_value() as usize);
        assert_eq!(*first_out.as_slice().first().unwrap(), 0);
        assert_eq!(*first_out.as_slice().last().unwrap() as usize, head.as_slice().len());
        assert_eq!(weight.as_slice().len(), head.as_slice().len());

        FirstOutGraph { first_out, head, weight }
    }

    /// Decompose the graph into its three seperate data containers
    pub fn decompose(self) -> (FirstOutContainer, HeadContainer, WeightContainer) {
        (self.first_out, self.head, self.weight)
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> Deconstruct for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("first_out", &self.first_out())?;
        store("head", &self.head())?;
        store("weights", &self.weight())?;
        Ok(())
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
            .flat_map(|neighbors| neighbors.into_iter().map(|Link { node, weight }| (node, weight)))
            .unzip();

        OwnedGraph::new(first_out, head, weight)
    }
}

impl<G: for<'a> LinkIterable<'a, Link>> BuildReversed<G> for OwnedGraph {
    fn reversed(graph: &G) -> Self {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<Link>> = (0..graph.num_nodes()).map(|_| Vec::<Link>::new()).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(graph.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in graph.link_iter(node) {
                reversed[neighbor as usize].push(Link { node, weight });
            }
        }

        OwnedGraph::from_adjancecy_lists(reversed)
    }
}

impl<G: for<'a> LinkIterable<'a, Link>> BuildPermutated<G> for OwnedGraph {
    fn permutated_filtered(graph: &G, order: &NodeOrder, mut predicate: Box<dyn FnMut(NodeId, NodeId) -> bool>) -> Self {
        let mut first_out: Vec<EdgeId> = Vec::with_capacity(graph.num_nodes() + 1);
        first_out.push(0);
        let mut head = Vec::with_capacity(graph.num_arcs());
        let mut weight = Vec::with_capacity(graph.num_arcs());

        for (rank, &node) in order.order().iter().enumerate() {
            let mut links = graph
                .link_iter(node)
                .filter(|l| predicate(rank as NodeId, order.rank(l.node)))
                .collect::<Vec<_>>();
            first_out.push(first_out.last().unwrap() + links.len() as EdgeId);
            links.sort_unstable_by_key(|l| order.rank(l.node));

            for link in links {
                head.push(order.rank(link.node));
                weight.push(link.weight);
            }
        }

        OwnedGraph::new(first_out, head, weight)
    }
}

impl<FirstOutContainer, HeadContainer> FirstOutGraph<FirstOutContainer, HeadContainer, Vec<Weight>>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    pub fn swap_weights(&mut self, mut new_weights: &mut Vec<Weight>) {
        assert!(new_weights.len() == self.weight.len());
        swap(&mut self.weight, &mut new_weights);
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> Graph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
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

    fn degree(&self, node: NodeId) -> usize {
        let node = node as usize;
        (self.first_out()[node + 1] - self.first_out()[node]) as usize
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> LinkIterable<'a, Link> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    #[allow(clippy::type_complexity)]
    type Iter = std::iter::Map<std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::Iter<'a, Weight>>, fn((&NodeId, &Weight)) -> Link>;

    #[inline]
    fn link_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head()[range.clone()]
            .iter()
            .zip(self.weight()[range].iter())
            .map(|(&neighbor, &weight)| Link { node: neighbor, weight })
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> LinkIterable<'a, NodeId> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    type Iter = std::iter::Cloned<std::slice::Iter<'a, NodeId>>;

    fn link_iter(&'a self, node: NodeId) -> Self::Iter {
        self.head()[self.neighbor_edge_indices_usize(node)].iter().cloned()
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer> MutLinkIterable<'a, (&'a NodeId, &'a mut Weight)>
    for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight> + AsMutSlice<Weight>,
{
    type Iter = std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::IterMut<'a, Weight>>;

    #[inline]
    fn link_iter_mut(&'a mut self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head.as_slice()[range.clone()].iter().zip(self.weight.as_mut_slice()[range].iter_mut())
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> RandomLinkAccessGraph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    #[inline]
    fn link(&self, edge_id: EdgeId) -> Link {
        Link {
            node: self.head()[edge_id as usize],
            weight: self.weight()[edge_id as usize],
        }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out()[from as usize];
        let range = self.neighbor_edge_indices_usize(from);
        self.head()[range].iter().position(|&head| head == to).map(|pos| pos as EdgeId + first_out)
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out()[node as usize] as EdgeId)..(self.first_out()[(node + 1) as usize] as EdgeId)
    }
}

/// Container struct for the collections of an unweighted graph.
/// Genric over the types of the data collections.
/// Anything that can be dereferenced to a slice works.
/// Both owned (`Vec<T>`, `Box<[T]>`) and shared (`Rc<[T]>`, `Arc<[T])>`) or borrowed (slices) data is possible.
#[derive(Debug, Clone)]
pub struct UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    // index of first edge of each node +1 entry in the end
    first_out: FirstOutContainer,
    // the node ids to which each edge points
    head: HeadContainer,
}

impl<FirstOutContainer, HeadContainer> UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    /// Borrow a slice of the first_out data
    pub fn first_out(&self) -> &[EdgeId] {
        self.first_out.as_slice()
    }
    /// Borrow a slice of the head data
    pub fn head(&self) -> &[NodeId] {
        self.head.as_slice()
    }

    /// Create a new `FirstOutGraph` from the three containers.
    pub fn new(first_out: FirstOutContainer, head: HeadContainer) -> Self {
        assert!(first_out.as_slice().len() < <NodeId>::max_value() as usize);
        assert!(head.as_slice().len() < <EdgeId>::max_value() as usize);
        assert_eq!(*first_out.as_slice().first().unwrap(), 0);
        assert_eq!(*first_out.as_slice().last().unwrap() as usize, head.as_slice().len());

        Self { first_out, head }
    }

    /// Decompose the graph into its seperate data containers
    pub fn decompose(self) -> (FirstOutContainer, HeadContainer) {
        (self.first_out, self.head)
    }
}

impl<FirstOutContainer, HeadContainer> Graph for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    fn num_nodes(&self) -> usize {
        self.first_out().len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head().len()
    }

    fn degree(&self, node: NodeId) -> usize {
        let node = node as usize;
        (self.first_out()[node + 1] - self.first_out()[node]) as usize
    }
}

impl<'a, FirstOutContainer, HeadContainer> LinkIterable<'a, NodeId> for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    type Iter = std::iter::Cloned<std::slice::Iter<'a, NodeId>>;

    fn link_iter(&'a self, node: NodeId) -> Self::Iter {
        self.head()[self.neighbor_edge_indices_usize(node)].iter().cloned()
    }
}

impl<FirstOutContainer, HeadContainer> RandomLinkAccessGraph for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
{
    #[inline]
    fn link(&self, edge_id: EdgeId) -> Link {
        Link {
            node: self.head()[edge_id as usize],
            weight: 0,
        }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out()[from as usize];
        let range = self.neighbor_edge_indices_usize(from);
        self.head()[range].iter().position(|&head| head == to).map(|pos| pos as EdgeId + first_out)
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out()[node as usize] as EdgeId)..(self.first_out()[(node + 1) as usize] as EdgeId)
    }
}

pub type UnweightedOwnedGraph = UnweightedFirstOutGraph<Vec<EdgeId>, Vec<NodeId>>;

impl UnweightedOwnedGraph {
    pub fn from_adjancecy_lists(adjancecy_lists: Vec<Vec<NodeId>>) -> Self {
        // create first_out array by doing a prefix sum over the adjancecy list sizes
        let first_out = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.len() as EdgeId);
            degrees_to_first_out(degrees).collect()
        };

        let head = adjancecy_lists.into_iter().flat_map(|neighbors| neighbors.into_iter()).collect();

        Self::new(first_out, head)
    }
}

impl<G: for<'a> LinkIterable<'a, NodeId>> BuildReversed<G> for UnweightedOwnedGraph {
    fn reversed(graph: &G) -> Self {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<NodeId>> = (0..graph.num_nodes()).map(|_| Vec::<NodeId>::new()).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(graph.num_nodes() as NodeId) {
            for neighbor in graph.link_iter(node) {
                reversed[neighbor as usize].push(node);
            }
        }

        Self::from_adjancecy_lists(reversed)
    }
}

/// Build a first_out array from an iterator of degrees
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
        let graph = FirstOutGraph::new(vec![0, 2, 3, 6, 8, 8, 8], vec![2, 1, 3, 1, 3, 4, 0, 4], vec![10, 1, 2, 1, 3, 1, 7, 2]);

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
        let expected = FirstOutGraph::new(vec![0, 1, 3, 4, 6, 8, 8], vec![3, 0, 2, 0, 1, 2, 2, 3], vec![7, 1, 1, 10, 2, 3, 1, 2]);
        let reversed = OwnedGraph::reversed(&graph);

        assert_eq!(reversed.first_out, expected.first_out);
        assert_eq!(reversed.head, expected.head);
        assert_eq!(reversed.weight, expected.weight);
    }
}
