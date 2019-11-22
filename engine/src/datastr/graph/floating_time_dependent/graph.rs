use super::*;
use crate::datastr::graph::time_dependent::period as int_period;
use crate::datastr::graph::Graph as GraphTrait;

type IPPIndex = u32;

#[derive(Debug, Clone)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipps: Vec<TTFPoint>,
}

impl Graph {
    pub fn new(
        first_out: Vec<EdgeId>,
        head: Vec<NodeId>,
        mut first_ipp_of_arc: Vec<IPPIndex>,
        ipp_departure_time: Vec<u32>,
        ipp_travel_time: Vec<u32>,
    ) -> Graph {
        let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
        let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

        let mut added = 0;

        for i in 0..head.len() {
            let range = first_ipp_of_arc[i] as usize..first_ipp_of_arc[i + 1] as usize;
            assert_ne!(range.start, range.end);

            first_ipp_of_arc[i] += added;

            if range.end - range.start > 1 {
                if ipp_departure_time[range.start] != 0 {
                    new_ipp_departure_time.push(0);
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
                new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
                new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
                new_ipp_departure_time.push(int_period());
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
                added += 1;
            } else {
                new_ipp_departure_time.push(0);
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
            }
        }
        first_ipp_of_arc[head.len()] += added;

        let ipps = new_ipp_departure_time
            .into_iter()
            .zip(new_ipp_travel_time.into_iter())
            .map(|(dt, tt)| TTFPoint {
                at: Timestamp::new(f64::from(dt) / 1000.0),
                val: FlWeight::new(f64::from(tt) / 1000.0),
            })
            .collect();

        Graph {
            first_out,
            head,
            first_ipp_of_arc,
            ipps,
        }
    }

    pub fn travel_time_function(&self, edge_id: EdgeId) -> PiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PiecewiseLinearFunction::new(&self.ipps[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize])
    }

    pub fn neighbor_and_edge_id_iter(&self, node: NodeId) -> impl Iterator<Item = (&NodeId, EdgeId)> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().zip(self.neighbor_edge_indices(node))
    }

    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }

    pub fn check_path(&self, path: Vec<(NodeId, Timestamp)>) {
        let mut iter = path.into_iter();
        let mut prev = iter.next().unwrap();
        for (node, t) in iter {
            let (prev_node, prev_t) = prev;
            let edge = self.edge_index(prev_node, node).expect("path contained nonexisting edge");
            let evaled = prev_t + self.travel_time_function(edge).evaluate(prev_t);
            assert!(
                t.fuzzy_eq(evaled),
                "expected {:?} - got {:?} at edge {} from {} (at {:?}) to {}",
                evaled,
                t,
                edge,
                prev_node,
                prev_t,
                node
            );
            prev = (node, t);
        }
    }

    pub fn num_ipps(&self) -> usize {
        self.ipps.len()
    }

    pub fn num_constant(&self) -> usize {
        self.first_ipp_of_arc
            .windows(2)
            .map(|firsts| firsts[1] - firsts[0])
            .filter(|&deg| deg == 1)
            .count()
    }
}

impl GraphTrait for Graph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head.len()
    }
}

impl<'a> LinkIterGraph<'a> for Graph {
    type Iter = std::iter::Map<std::slice::Iter<'a, NodeId>, fn(&NodeId) -> Link>;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().map(|&head| Link { node: head, weight: 0 })
    }
}

impl RandomLinkAccessGraph for Graph {
    fn link(&self, edge_id: EdgeId) -> Link {
        Link {
            node: self.head[edge_id as usize],
            weight: 0,
        }
    }

    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId> {
        let first_out = self.first_out[from as usize] as usize;
        self.neighbor_iter(from)
            .enumerate()
            .find(|&(_, Link { node, .. })| node == to)
            .map(|(i, _)| (first_out + i) as EdgeId)
    }

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }
}
