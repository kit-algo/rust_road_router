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
use crate::io::*;
use crate::report::*;
use crate::util::*;
use std::mem::swap;

/// Container struct for the three collections of a graph.
/// Genric over the types of the three data collections.
/// Anything that can be dereferenced to a slice works.
/// Both owned (`Vec<T>`, `Box<[T]>`) and shared (`Rc<[T]>`, `Arc<[T])>`) or borrowed (slices) data is possible.
#[derive(Debug, Clone)]
pub struct FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W = Weight> {
    // index of first edge of each node +1 entry in the end
    first_out: FirstOutContainer,
    // the node ids to which each edge points
    head: HeadContainer,
    // the weight of each edge
    weight: WeightContainer,
    _phantom: std::marker::PhantomData<W>,
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
{
    /// Borrow a slice of the first_out data
    pub fn first_out(&self) -> &[EdgeId] {
        self.first_out.as_ref()
    }
    /// Borrow a slice of the head data
    pub fn head(&self) -> &[NodeId] {
        self.head.as_ref()
    }
    /// Borrow a slice of the weight data
    pub fn weight(&self) -> &[W] {
        self.weight.as_ref()
    }

    /// Create a new `FirstOutGraph` from the three containers.
    pub fn new(first_out: FirstOutContainer, head: HeadContainer, weight: WeightContainer) -> Self {
        assert!(first_out.as_ref().len() < <NodeId>::max_value() as usize);
        assert!(head.as_ref().len() < <EdgeId>::max_value() as usize);
        assert_eq!(*first_out.as_ref().first().unwrap(), 0);
        assert_eq!(*first_out.as_ref().last().unwrap() as usize, head.as_ref().len());
        assert_eq!(weight.as_ref().len(), head.as_ref().len());

        Self {
            first_out,
            head,
            weight,
            _phantom: Default::default(),
        }
    }

    /// Decompose the graph into its three seperate data containers
    pub fn decompose(self) -> (FirstOutContainer, HeadContainer, WeightContainer) {
        (self.first_out, self.head, self.weight)
    }

    pub fn borrowed(&self) -> FirstOutGraph<&[EdgeId], &[NodeId], &[W]> {
        FirstOutGraph {
            first_out: self.first_out(),
            head: self.head(),
            weight: self.weight(),
            _phantom: Default::default(),
        }
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> Deconstruct for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[Weight]>,
{
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("first_out", &self.first_out())?;
        store("head", &self.head())?;
        store("weights", &self.weight())?;
        Ok(())
    }
}

pub type OwnedGraph = FirstOutGraph<Vec<EdgeId>, Vec<NodeId>, Vec<Weight>>;
pub type BorrowedGraph<'a, W = Weight> = FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [W], W>;

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

impl<G: LinkIterable<Link>> BuildReversed<G> for OwnedGraph {
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

impl<G: LinkIterable<Link>> BuildPermutated<G> for OwnedGraph {
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

pub struct WeightedGraphReconstructor(pub &'static str);

impl ReconstructPrepared<OwnedGraph> for WeightedGraphReconstructor {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<OwnedGraph> {
        let g = OwnedGraph::new(loader.load("first_out")?, loader.load("head")?, loader.load(self.0)?);
        report!("graph", { "num_nodes": g.num_nodes(), "num_arcs": g.num_arcs() });
        Ok(g)
    }
}

impl<FirstOutContainer, HeadContainer> FirstOutGraph<FirstOutContainer, HeadContainer, Vec<Weight>> {
    pub fn swap_weights(&mut self, new_weights: &mut Vec<Weight>) {
        assert!(new_weights.len() == self.weight.len());
        swap(&mut self.weight, new_weights);
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> Graph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
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

impl<FirstOutContainer, HeadContainer, WeightContainer> LinkIterable<Link> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[Weight]>,
{
    #[allow(clippy::type_complexity)]
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = Link> + 'a;

    #[inline]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = SlcsIdx(self.first_out()).range(node as usize);
        self.head()[range.clone()]
            .iter()
            .zip(self.weight()[range].iter())
            .map(|(&neighbor, &weight)| Link { node: neighbor, weight })
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> LinkIterable<NodeIdT> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
{
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = NodeIdT> + 'a;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.head()[SlcsIdx(self.first_out()).range(node as usize)].iter().copied().map(NodeIdT)
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer, W> MutLinkIterable<'a, (&'a NodeId, &'a mut W)>
    for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]> + AsMut<[W]>,
{
    type Iter = std::iter::Zip<std::slice::Iter<'a, NodeId>, std::slice::IterMut<'a, W>>;

    #[inline]
    fn link_iter_mut(&'a mut self, node: NodeId) -> Self::Iter {
        let range = SlcsIdx(self.first_out()).range(node as usize);
        self.head.as_ref()[range.clone()].iter().zip(self.weight.as_mut()[range].iter_mut())
    }
}

impl<'a, FirstOutContainer, HeadContainer, WeightContainer, W> FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    WeightContainer: AsMut<[W]>,
{
    pub fn weights_mut(&mut self) -> &mut [W] {
        self.weight.as_mut()
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> EdgeIdGraph for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
{
    type IdxIter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = EdgeIdT> + 'a;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.neighbor_edge_indices(from).filter(move |&e| self.head()[e as usize] == to).map(EdgeIdT)
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out()[node as usize] as EdgeId)..(self.first_out()[(node + 1) as usize] as EdgeId)
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer> EdgeRandomAccessGraph<Link> for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[Weight]>,
{
    #[inline]
    fn link(&self, edge_id: EdgeId) -> Link {
        Link {
            node: self.head()[edge_id as usize],
            weight: self.weight()[edge_id as usize],
        }
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> LinkIterable<(NodeIdT, EdgeIdT)>
    for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
{
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = (NodeIdT, EdgeIdT)> + 'a;

    #[inline]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = SlcsIdx(self.first_out()).range(node as usize);
        self.head()[range.clone()]
            .iter()
            .zip(range)
            .map(|(&node, e)| (NodeIdT(node), EdgeIdT(e as EdgeId)))
    }
}

impl<FirstOutContainer, HeadContainer, WeightContainer, W> LinkIterable<(NodeIdT, W, EdgeIdT)>
    for FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer, W>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
    WeightContainer: AsRef<[W]>,
    W: Copy,
{
    #[allow(clippy::type_complexity)]
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = (NodeIdT, W, EdgeIdT)> + 'a;

    #[inline]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = SlcsIdx(self.first_out()).range(node as usize);
        self.head()[range.clone()]
            .iter()
            .zip(self.weight()[range.clone()].iter())
            .zip(range)
            .map(|((&node, &weight), e)| (NodeIdT(node), weight, EdgeIdT(e as EdgeId)))
    }
}

/// Container struct for the collections of an unweighted graph.
/// Genric over the types of the data collections.
/// Anything that can be dereferenced to a slice works.
/// Both owned (`Vec<T>`, `Box<[T]>`) and shared (`Rc<[T]>`, `Arc<[T])>`) or borrowed (slices) data is possible.
#[derive(Clone)]
pub struct UnweightedFirstOutGraph<FirstOutContainer, HeadContainer> {
    // index of first edge of each node +1 entry in the end
    first_out: FirstOutContainer,
    // the node ids to which each edge points
    head: HeadContainer,
}

impl<FirstOutContainer, HeadContainer> UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
{
    /// Borrow a slice of the first_out data
    pub fn first_out(&self) -> &[EdgeId] {
        self.first_out.as_ref()
    }
    /// Borrow a slice of the head data
    pub fn head(&self) -> &[NodeId] {
        self.head.as_ref()
    }

    /// Create a new `FirstOutGraph` from the three containers.
    pub fn new(first_out: FirstOutContainer, head: HeadContainer) -> Self {
        assert!(first_out.as_ref().len() < <NodeId>::max_value() as usize);
        assert!(head.as_ref().len() < <EdgeId>::max_value() as usize);
        assert_eq!(*first_out.as_ref().first().unwrap(), 0);
        assert_eq!(*first_out.as_ref().last().unwrap() as usize, head.as_ref().len());

        Self { first_out, head }
    }

    /// Decompose the graph into its seperate data containers
    pub fn decompose(self) -> (FirstOutContainer, HeadContainer) {
        (self.first_out, self.head)
    }
}

impl<FirstOutContainer, HeadContainer> Graph for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
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

impl<FirstOutContainer, HeadContainer> LinkIterable<NodeIdT> for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
{
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = NodeIdT>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.head()[self.neighbor_edge_indices_usize(node)].iter().copied().map(NodeIdT)
    }
}

impl<FirstOutContainer, HeadContainer> LinkIterable<(NodeIdT, EdgeIdT)> for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
{
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = (NodeIdT, EdgeIdT)> + 'a;

    #[inline]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = SlcsIdx(self.first_out()).range(node as usize);
        self.head()[range.clone()]
            .iter()
            .zip(range)
            .map(|(&node, e)| (NodeIdT(node), EdgeIdT(e as EdgeId)))
    }
}

impl<FirstOutContainer, HeadContainer> EdgeIdGraph for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
{
    // https://github.com/rust-lang/rustfmt/issues/4911
    #[rustfmt::skip]
    type IdxIter<'a> where Self: 'a = impl Iterator<Item = EdgeIdT> + 'a;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.neighbor_edge_indices(from).filter(move |&e| self.head()[e as usize] == to).map(EdgeIdT)
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out()[node as usize] as EdgeId)..(self.first_out()[(node + 1) as usize] as EdgeId)
    }
}

impl<FirstOutContainer, HeadContainer> EdgeRandomAccessGraph<NodeIdT> for UnweightedFirstOutGraph<FirstOutContainer, HeadContainer>
where
    FirstOutContainer: AsRef<[EdgeId]>,
    HeadContainer: AsRef<[NodeId]>,
{
    #[inline]
    fn link(&self, edge_id: EdgeId) -> NodeIdT {
        NodeIdT(self.head()[edge_id as usize])
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

impl<G: LinkIterable<NodeIdT>> BuildReversed<G> for UnweightedOwnedGraph {
    fn reversed(graph: &G) -> Self {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<NodeId>> = (0..graph.num_nodes()).map(|_| Vec::<NodeId>::new()).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(graph.num_nodes() as NodeId) {
            for NodeIdT(neighbor) in graph.link_iter(node) {
                reversed[neighbor as usize].push(node);
            }
        }

        Self::from_adjancecy_lists(reversed)
    }
}

impl Reconstruct for UnweightedOwnedGraph {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        let g = Self::new(loader.load("first_out")?, loader.load("head")?);
        report!("graph", { "num_nodes": g.num_nodes(), "num_arcs": g.num_arcs() });
        Ok(g)
    }
}

pub struct ReversedGraphWithEdgeIds {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    edge_ids: Vec<EdgeId>,
}

impl<G: LinkIterable<(NodeIdT, EdgeIdT)>> BuildReversed<G> for ReversedGraphWithEdgeIds {
    fn reversed(graph: &G) -> Self {
        let mut reversed_first_out = vec![0u32; graph.num_nodes() + 1];
        for node in 0..(graph.num_nodes() as NodeId) {
            for (NodeIdT(neighbor), _) in graph.link_iter(node) {
                reversed_first_out[neighbor as usize + 1] += 1;
            }
        }

        let mut prefix_sum = 0;
        for deg in &mut reversed_first_out {
            prefix_sum += *deg;
            *deg = prefix_sum;
        }

        let mut head = vec![0; graph.num_arcs()];
        let mut edge_ids = vec![0; graph.num_arcs()];

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(graph.num_nodes() as NodeId) {
            for (NodeIdT(neighbor), EdgeIdT(edge_id)) in graph.link_iter(node) {
                let idx = reversed_first_out[neighbor as usize] as usize;
                reversed_first_out[neighbor as usize] += 1;
                head[idx] = node;
                edge_ids[idx] = edge_id;
            }
        }

        for node in (1..graph.num_nodes()).rev() {
            reversed_first_out[node] = reversed_first_out[node - 1];
        }
        reversed_first_out[0] = 0;

        ReversedGraphWithEdgeIds {
            first_out: reversed_first_out,
            head,
            edge_ids,
        }
    }
}

impl Graph for ReversedGraphWithEdgeIds {
    fn degree(&self, node: NodeId) -> usize {
        SlcsIdx(&self.first_out).range(node as usize).len()
    }
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }
    fn num_arcs(&self) -> usize {
        self.head.len()
    }
}

impl LinkIterable<(NodeIdT, Reversed)> for ReversedGraphWithEdgeIds {
    type Iter<'a> = impl Iterator<Item = (NodeIdT, Reversed)> + 'a;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = SlcsIdx(&self.first_out).range(node as usize);
        self.head[range.clone()]
            .iter()
            .copied()
            .map(NodeIdT)
            .zip(self.edge_ids[range].iter().copied().map(EdgeIdT).map(Reversed))
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
