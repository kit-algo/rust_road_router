use super::*;

pub struct SeperatorBasedParallelCustomization<'a, T, F, G> {
    cch: &'a CCH,
    separators: SeparatorTree,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
}

impl<'a, T, F, G> SeperatorBasedParallelCustomization<'a, T, F, G> where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
    G: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
{
    pub fn new(cch: &'a CCH, customize_cell: F, customize_separator: G) -> Self {
        let separators = cch.separators();
        if cfg!(feature = "cch-disable-par") {
            separators.validate_for_parallelization();
        }

        SeperatorBasedParallelCustomization {
            cch, separators, customize_cell, customize_separator, _t: std::marker::PhantomData::<T>
        }
    }

    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T]) {
        if cfg!(feature = "cch-disable-par") {
            (self.customize_cell)(0..self.cch.num_nodes(), 0, upward, downward);
        } else {
            self.customize_tree(&self.separators, 0, upward, downward);
        }
    }

    fn customize_tree(&self, sep_tree: &SeparatorTree, offset: usize, upward: &'a mut [T], downward: &'a mut [T]) {
        let edge_offset = self.cch.first_out[offset] as usize;

        if sep_tree.num_nodes < self.cch.num_nodes() / (32 * rayon::current_num_threads()) {
            (self.customize_cell)(offset..offset + sep_tree.num_nodes, edge_offset, upward, downward);
        } else {
            let mut sub_offset = offset;
            let mut sub_edge_offset = edge_offset;
            let mut sub_upward = &mut upward[..];
            let mut sub_downward = &mut downward[..];

            rayon::scope(|s| {
                for sub in &sep_tree.children {
                    let (this_sub_up, rest_up) = (move || { sub_upward })().split_at_mut(self.cch.first_out[sub_offset + sub.num_nodes] as usize - sub_edge_offset);
                    let (this_sub_down, rest_down) = (move || { sub_downward })().split_at_mut(self.cch.first_out[sub_offset + sub.num_nodes] as usize - sub_edge_offset);
                    sub_edge_offset += this_sub_up.len();
                    if sub.num_nodes < self.cch.num_nodes() / (32 * rayon::current_num_threads()) {
                        self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down);
                    } else {
                        s.spawn(move |_| self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down));
                    }
                    sub_offset += sub.num_nodes;
                    sub_upward = rest_up;
                    sub_downward = rest_down;
                }
            });

            (self.customize_separator)(sub_offset..offset + sep_tree.num_nodes, edge_offset, upward, downward)
        }
    }
}
