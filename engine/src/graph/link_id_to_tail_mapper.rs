use super::*;
use crate::as_slice::AsSlice;
use crate::rank_select_map::*;

#[derive(Debug)]
pub struct LinkIdToTailMapper {
    link_id_to_tail: RankSelectMap,
    deg_zero_nodes_indices: Vec<EdgeId>,
}

impl LinkIdToTailMapper {
    pub fn new<FOC, HC, WC>(graph: &FirstOutGraph<FOC, HC, WC>) -> LinkIdToTailMapper
    where
        FOC: AsSlice<EdgeId>,
        HC: AsSlice<NodeId>,
        WC: AsSlice<Weight>,
    {
        let mut first_out_bits = BitVec::new(graph.num_arcs() + 1);
        let mut deg_zero_nodes_indices = Vec::new();
        let mut counter: EdgeId = 0;

        for node in 0..graph.num_nodes() {
            let deg = graph.degree(node as NodeId) as EdgeId;
            if deg == 0 {
                deg_zero_nodes_indices.push(counter);
            } else {
                first_out_bits.set(counter as usize);
            }
            counter += deg;
        }
        let link_id_to_tail = RankSelectMap::new(first_out_bits);

        let mapper = LinkIdToTailMapper {
            link_id_to_tail,
            deg_zero_nodes_indices,
        };

        counter = 0;
        for node in 0..graph.num_nodes() {
            let deg = graph.degree(node as NodeId) as EdgeId;
            for link_id in 0..deg {
                debug_assert_eq!(mapper.link_id_to_tail(counter + link_id), node as NodeId);
            }
            counter += deg;
        }
        mapper
    }

    pub fn link_id_to_tail(&self, link_id: EdgeId) -> NodeId {
        let num_deg_zeros_below = match self.deg_zero_nodes_indices.binary_search(&link_id) {
            Ok(ref mut index) => {
                while *index < self.deg_zero_nodes_indices.len() && self.deg_zero_nodes_indices[*index] == link_id {
                    *index += 1;
                }
                *index
            }
            Err(index) => index,
        };
        self.link_id_to_tail.at_or_next_lower(link_id as usize) as NodeId + num_deg_zeros_below as NodeId
    }
}
