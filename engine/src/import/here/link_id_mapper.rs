use super::*;
use crate::datastr::rank_select_map::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkDirection {
    FromRef,
    ToRef,
}

pub struct LinkIdMapper {
    link_id_mapping: InvertableRankSelectMap,
    here_rank_to_link_id: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    link_id_to_here_rank: Vec<EdgeId>,
}

impl LinkIdMapper {
    pub fn new(
        link_id_mapping: InvertableRankSelectMap,
        here_rank_to_link_id: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
        num_arcs: usize,
    ) -> LinkIdMapper {
        let mut link_id_to_here_rank = vec![0; num_arcs];
        for (rank, &(from_ref, to_ref)) in here_rank_to_link_id.iter().enumerate() {
            if let Some(link_id) = from_ref.value() {
                link_id_to_here_rank[link_id as usize] = rank as EdgeId;
            }
            if let Some(link_id) = to_ref.value() {
                link_id_to_here_rank[link_id as usize] = rank as EdgeId;
            }
        }
        LinkIdMapper {
            link_id_mapping,
            here_rank_to_link_id,
            link_id_to_here_rank,
        }
    }

    pub fn here_to_local_link_id(&self, here_link_id: u64, direction: LinkDirection) -> Option<EdgeId> {
        let rank = self.link_id_mapping.get(here_link_id as usize)?;
        match direction {
            LinkDirection::FromRef => self.here_rank_to_link_id[rank].0.value(),
            LinkDirection::ToRef => self.here_rank_to_link_id[rank].1.value(),
        }
    }

    pub fn local_to_here_link_id(&self, link_id: EdgeId) -> (u64, LinkDirection) {
        let rank = self.link_id_to_here_rank[link_id as usize] as usize;
        let here_link_id = self.link_id_mapping.inverse(rank) as u64;
        let direction = if self.here_rank_to_link_id[rank].0.value() == Some(link_id) {
            LinkDirection::FromRef
        } else {
            debug_assert_eq!(self.here_rank_to_link_id[rank].0.value(), Some(link_id));
            LinkDirection::ToRef
        };
        (here_link_id, direction)
    }
}
