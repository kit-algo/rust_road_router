//! Separator based parallelization of CCH customization.
//! Utilizes that disconnected cells (by removing a separator) can be processed independently.

use super::*;

/// Parallelization of basic customization.
pub struct SeperatorBasedParallelCustomization<'a, T, F, G, C> {
    cch: &'a C,
    separators: SeparatorTree,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
}

pub fn new_undirected_parallelization<T: Send + Sync>(
    cch: &CCH,
    customize_cell: impl Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
    customize_separator: impl Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
) -> SeperatorBasedParallelCustomization<
    T,
    impl Sync + Fn(Range<usize>, usize, usize, &mut [T], &mut [T]),
    impl Sync + Fn(Range<usize>, usize, usize, &mut [T], &mut [T]),
    CCH,
> {
    SeperatorBasedParallelCustomization::new(
        cch,
        move |r, o_up, o_dd, up, down| {
            debug_assert_eq!(o_up, o_dd);
            customize_cell(r, o_up, up, down)
        },
        move |r, o_up, o_dd, up, down| {
            debug_assert_eq!(o_up, o_dd);
            customize_separator(r, o_up, up, down)
        },
    )
}

impl<'a, T, F, G, C> SeperatorBasedParallelCustomization<'a, T, F, G, C>
where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, usize, usize, &mut [T], &mut [T]),
    G: Sync + Fn(Range<usize>, usize, usize, &mut [T], &mut [T]),
    C: CCHT + Sync,
{
    /// Setup for parallelization, we need a cch, and a routine for customization of cells
    /// and one for customization of separators.
    /// These should do the same thing in the end, but may achieve it in different ways because there are different performance trade-offs.
    /// The cell routine will be invoked several times in parallel and nodes will mostly have low degrees.
    /// The separator routine will be invoked only very few times in parallel, the final separator will be customized completely alone and nodes have high degrees.
    pub fn new(cch: &'a C, customize_cell: F, customize_separator: G) -> Self {
        let separators = cch.separators();
        if !cfg!(feature = "cch-disable-par") {
            separators.validate_for_parallelization();
        }

        Self {
            cch,
            separators,
            customize_cell,
            customize_separator,
            _t: std::marker::PhantomData::<T>,
        }
    }

    /// Execute customization. Takes a mut slice to the full memory where weights that should be customized are stored.
    /// The setup callback can be used to perform additional scoped setup work.
    /// It has to call the callback that gets passed to it in turn.
    /// Otherwise nothing will happen.
    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T], setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync) {
        if cfg!(feature = "cch-disable-par") {
            setup(Box::new(|| {
                (self.customize_cell)(0..self.cch.forward_first_out().len() - 1, 0, 0, upward, downward)
            }));
        } else {
            let core_ids = core_affinity::get_core_ids().unwrap();
            rayon::ThreadPoolBuilder::new()
                .build_scoped(
                    |thread| {
                        core_affinity::set_for_current(core_ids[thread.index()]);
                        setup(Box::new(|| thread.run()));
                    },
                    |pool| pool.install(|| self.customize_tree(&self.separators, 0, upward, downward)),
                )
                .unwrap();
        }
    }

    fn customize_tree(&self, sep_tree: &SeparatorTree, offset: usize, upward: &'a mut [T], downward: &'a mut [T]) {
        let n = self.cch.forward_first_out().len() - 1;
        let forward_edge_offset = self.cch.forward_first_out()[offset] as usize;
        let backward_edge_offset = self.cch.backward_first_out()[offset] as usize;

        if sep_tree.num_nodes < n / (32 * rayon::current_num_threads()) {
            // if the current cell is small enough (load balancing parameters) run the customize_cell routine on it
            (self.customize_cell)(offset..offset + sep_tree.num_nodes, forward_edge_offset, backward_edge_offset, upward, downward);
        } else {
            // if not, split at the separator, process all subcells independently in parallel and the separator afterwards
            let mut sub_offset = offset;
            let mut sub_forward_edge_offset = forward_edge_offset;
            let mut sub_backward_edge_offset = backward_edge_offset;
            let mut sub_upward = &mut *upward;
            let mut sub_downward = &mut *downward;

            rayon::scope(|s| {
                for sub in &sep_tree.children {
                    let (this_sub_up, rest_up) =
                        (move || sub_upward)().split_at_mut(self.cch.forward_first_out()[sub_offset + sub.num_nodes] as usize - sub_forward_edge_offset);
                    let (this_sub_down, rest_down) =
                        (move || sub_downward)().split_at_mut(self.cch.backward_first_out()[sub_offset + sub.num_nodes] as usize - sub_backward_edge_offset);
                    sub_forward_edge_offset += this_sub_up.len();
                    sub_backward_edge_offset += this_sub_down.len();
                    // if the subcell is small enough don't bother spawning a thread
                    // this catches the case of very small cell at high levels which may sometime occur
                    // subcells are ordered descending by their size, so we will always first spawn of tasks for the big ones
                    if sub.num_nodes < n / (32 * rayon::current_num_threads()) {
                        self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down);
                    } else {
                        s.spawn(move |_| self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down));
                    }
                    sub_offset += sub.num_nodes;
                    sub_upward = rest_up;
                    sub_downward = rest_down;
                }
            });

            // once all subcells are processed, process the separator itself
            (self.customize_separator)(
                sub_offset..offset + sep_tree.num_nodes,
                forward_edge_offset,
                backward_edge_offset,
                upward,
                downward,
            )
        }
    }
}

// Ugly unsafe stuff for perfect customization.
// We basically need many threads to mutable access the weights arrays at the same time.
// We can't reasonably split everything into consecutive slice, so we just distribute the whole
// thing as a pointer and rely on unsafe.
// Theoretically we can guarantee that we will never modify anything concurrently,
// but I haven't found a way to express that in safe rust.
struct PtrWrapper<T>(*mut T);
unsafe impl<T> Send for PtrWrapper<T> {}
unsafe impl<T> Sync for PtrWrapper<T> {}
impl<T> Clone for PtrWrapper<T> {
    fn clone(&self) -> Self {
        PtrWrapper(self.0)
    }
}
impl<T> Copy for PtrWrapper<T> {}

pub struct SeperatorBasedPerfectParallelCustomization<'a, T, F, G> {
    cch: &'a CCH,
    separators: SeparatorTree,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
}

/// Parallelization of perfect customization.
impl<'a, T, F, G> SeperatorBasedPerfectParallelCustomization<'a, T, F, G>
where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, *mut T, *mut T),
    G: Sync + Fn(Range<usize>, *mut T, *mut T),
{
    /// Setup for parallelization, we need a cch, and a routine for customization of cells
    /// and one for customization of separators.
    /// These should do the same thing in the end, but may achieve it in different ways because there are different performance trade-offs.
    /// The cell routine will be invoked several times in parallel and nodes will mostly have low degrees.
    /// The separator routine will be invoked only very few times in parallel, the final separator will be customized completely alone and nodes have high degrees.
    pub fn new(cch: &'a CCH, customize_cell: F, customize_separator: G) -> Self {
        let separators = cch.separators();
        if cfg!(feature = "cch-disable-par") {
            separators.validate_for_parallelization();
        }

        SeperatorBasedPerfectParallelCustomization {
            cch,
            separators,
            customize_cell,
            customize_separator,
            _t: std::marker::PhantomData::<T>,
        }
    }

    /// Execute customization. Takes a mut slice to the full memory where weights that should be customized are stored.
    /// The setup callback can be used to perform additional scoped setup work.
    /// It has to call the callback that gets passed to it in turn.
    /// Otherwise nothing will happen.
    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T], setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync) {
        if cfg!(feature = "cch-disable-par") {
            setup(Box::new(|| {
                (self.customize_cell)(0..self.cch.num_nodes(), upward.as_mut_ptr(), downward.as_mut_ptr())
            }));
        } else {
            let core_ids = core_affinity::get_core_ids().unwrap();
            rayon::ThreadPoolBuilder::new()
                .build_scoped(
                    |thread| {
                        core_affinity::set_for_current(core_ids[thread.index()]);
                        setup(Box::new(|| thread.run()));
                    },
                    |pool| pool.install(|| self.customize_tree(&self.separators, self.cch.num_nodes(), upward.as_mut_ptr(), downward.as_mut_ptr())),
                )
                .unwrap();
        }
    }

    fn customize_tree(&self, sep_tree: &SeparatorTree, offset: usize, upward: *mut T, downward: *mut T) {
        if sep_tree.num_nodes < self.cch.num_nodes() / (32 * rayon::current_num_threads()) {
            // if the current cell is small enough (load balancing parameters) run the customize_cell routine on it
            (self.customize_cell)(offset - sep_tree.num_nodes..offset, upward, downward);
        } else {
            // if not, process separator, then split into subcells and process them independently in parallel.
            let mut end_next = offset - sep_tree.nodes.len();
            (self.customize_separator)(end_next..offset, upward, downward);
            let upward = PtrWrapper(upward);
            let downward = PtrWrapper(downward);

            rayon::scope(|s| {
                for sub in sep_tree.children.iter().rev() {
                    s.spawn(move |_| {
                        // force capturing the wrapper so closure is send/sync
                        let _ = &upward;
                        let _ = &downward;
                        self.customize_tree(sub, end_next, upward.0, downward.0)
                    });
                    end_next -= sub.num_nodes;
                }
            });
        }
    }
}
