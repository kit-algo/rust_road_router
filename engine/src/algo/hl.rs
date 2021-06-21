use super::*;

pub struct HubLabels {
    outgoing: Vec<Vec<(NodeId, Weight)>>,
    incoming: Vec<Vec<(NodeId, Weight)>>,
}

impl HubLabels {
    pub fn new<G, H>(up: &G, down: &H) -> Self
    where
        G: for<'a> LinkIterGraph<'a>,
        H: for<'a> LinkIterGraph<'a>,
    {
        HubLabels {
            outgoing: vec![Vec::new(); up.num_nodes()],
            incoming: vec![Vec::new(); up.num_nodes()],
        }
        .compute_labels_from_ch(up, down)
    }

    fn compute_labels_from_ch<G, H>(mut self, up: &G, down: &H) -> Self
    where
        G: for<'a> LinkIterGraph<'a>,
        H: for<'a> LinkIterGraph<'a>,
    {
        for node in (0..up.num_nodes()).rev() {
            self.outgoing[node].push((node as NodeId, 0));
            self.incoming[node].push((node as NodeId, 0));

            for link in LinkIterable::<Link>::link_iter(up, node as NodeId) {
                let (out_remaining, out_done) = self.outgoing.split_at_mut(node + 1);
                let cur_out = out_remaining.last_mut().unwrap();
                cur_out.extend(out_done[link.node as usize - node - 1].iter().map(|&(hub, weight)| (hub, weight + link.weight)));
            }

            for link in LinkIterable::<Link>::link_iter(down, node as NodeId) {
                let (in_remaining, in_done) = self.incoming.split_at_mut(node + 1);
                let cur_in = in_remaining.last_mut().unwrap();
                cur_in.extend(in_done[link.node as usize - node - 1].iter().map(|&(hub, weight)| (hub, weight + link.weight)));
            }

            for dir in [&mut self.outgoing[node], &mut self.incoming[node]] {
                dir.sort_unstable();
                for same_hub in dir.group_by_mut(|a, b| a.0 == b.0) {
                    let min_dist = same_hub.iter().map(|&(_, w)| w).min().unwrap();
                    for label in same_hub {
                        label.1 = min_dist;
                    }
                }
                dir.dedup();
            }

            self.outgoing[node] = self.outgoing[node]
                .iter()
                .filter_map(|&(hub, weight)| {
                    if Self::best_hub(&self.outgoing[node], &self.incoming[hub as usize])
                        .map(|(via, _)| via == hub)
                        .unwrap_or(true)
                    {
                        Some((hub, weight))
                    } else {
                        None
                    }
                })
                .collect();

            self.incoming[node] = self.incoming[node]
                .iter()
                .filter_map(|&(hub, weight)| {
                    if Self::best_hub(&self.outgoing[hub as usize], &self.incoming[node])
                        .map(|(via, _)| via == hub)
                        .unwrap_or(true)
                    {
                        Some((hub, weight))
                    } else {
                        None
                    }
                })
                .collect();
        }

        self
    }

    pub fn dist(&self, from: NodeId, to: NodeId) -> Option<Weight> {
        Self::best_hub(&self.outgoing[from as usize], &self.incoming[to as usize]).map(|(_, dist)| dist)
    }

    pub fn hub_and_dist(&self, from: NodeId, to: NodeId) -> Option<(NodeId, Weight)> {
        Self::best_hub(&self.outgoing[from as usize], &self.incoming[to as usize])
    }

    fn best_hub(from_out_labels: &[(NodeId, Weight)], to_in_labels: &[(NodeId, Weight)]) -> Option<(NodeId, Weight)> {
        let mut from_iter = from_out_labels.iter().peekable();
        let mut to_iter = to_in_labels.iter().peekable();

        let mut result = None;

        while let (Some(&&(forw_hub, forw_dist)), Some(&&(backw_hub, backw_dist))) = (from_iter.peek(), to_iter.peek()) {
            if forw_hub < backw_hub {
                from_iter.next();
            } else if backw_hub < forw_hub {
                to_iter.next();
            } else {
                if let Some((_, best_dist)) = result {
                    if forw_dist + backw_dist < best_dist {
                        result = Some((forw_hub, forw_dist + backw_dist));
                    }
                } else {
                    result = Some((forw_hub, forw_dist + backw_dist));
                }
                from_iter.next();
                to_iter.next();
            }
        }

        return result;
    }

    pub fn num_labels(&self) -> usize {
        self.outgoing.iter().map(|l| l.len()).sum::<usize>() + self.incoming.iter().map(|l| l.len()).sum::<usize>()
    }

    pub fn forward_labels(&self) -> &[Vec<(NodeId, Weight)>] {
        &self.outgoing
    }

    pub fn backward_labels(&self) -> &[Vec<(NodeId, Weight)>] {
        &self.incoming
    }
}
