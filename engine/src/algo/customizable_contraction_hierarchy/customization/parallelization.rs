//! Separator based parallelization of CCH customization.
//! Utilizes that disconnected cells (by removing a separator) can be processed independently.

use super::*;

pub trait SubgraphCustomization<T, E>: Sync {
    fn exec(
        &self,
        nodes: Range<usize>,
        edge_offset_up: usize,
        edge_offset_down: usize,
        edges_up: &mut [T],
        edges_down: &mut [T],
        aux_up: &mut [E],
        aux_down: &mut [E],
    );
}

pub struct UndirectedNoAux<F>(F);
impl<F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]), T, E> SubgraphCustomization<T, E> for UndirectedNoAux<F> {
    fn exec(
        &self,
        nodes: Range<usize>,
        edge_offset_up: usize,
        edge_offset_down: usize,
        edges_up: &mut [T],
        edges_down: &mut [T],
        _aux_up: &mut [E],
        _aux_down: &mut [E],
    ) {
        debug_assert_eq!(edge_offset_up, edge_offset_down);
        (self.0)(nodes, edge_offset_up, edges_up, edges_down)
    }
}

pub struct UndirectedWithAux<F>(F);
impl<F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T], &mut [E], &mut [E]), T, E> SubgraphCustomization<T, E> for UndirectedWithAux<F> {
    fn exec(
        &self,
        nodes: Range<usize>,
        edge_offset_up: usize,
        edge_offset_down: usize,
        edges_up: &mut [T],
        edges_down: &mut [T],
        aux_up: &mut [E],
        aux_down: &mut [E],
    ) {
        debug_assert_eq!(edge_offset_up, edge_offset_down);
        (self.0)(nodes, edge_offset_up, edges_up, edges_down, aux_up, aux_down)
    }
}

impl<F: Sync + Fn(Range<usize>, usize, usize, &mut [T], &mut [T], &mut [E], &mut [E]), T, E> SubgraphCustomization<T, E> for F {
    fn exec(
        &self,
        nodes: Range<usize>,
        edge_offset_up: usize,
        edge_offset_down: usize,
        edges_up: &mut [T],
        edges_down: &mut [T],
        aux_up: &mut [E],
        aux_down: &mut [E],
    ) {
        (self)(nodes, edge_offset_up, edge_offset_down, edges_up, edges_down, aux_up, aux_down)
    }
}

/// Parallelization of basic customization.
pub struct SeperatorBasedParallelCustomization<'a, T, F, G, C, E = ()> {
    cch: &'a C,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
    _e: std::marker::PhantomData<E>,
}

impl<'a, T, F, G> SeperatorBasedParallelCustomization<'a, T, UndirectedNoAux<F>, UndirectedNoAux<G>, CCH>
where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
    G: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
{
    pub fn new_undirected(cch: &'a CCH, customize_cell: F, customize_separator: G) -> Self {
        Self::new_with_aux(cch, UndirectedNoAux(customize_cell), UndirectedNoAux(customize_separator))
    }
}

impl<'a, T, F, G, E> SeperatorBasedParallelCustomization<'a, T, UndirectedWithAux<F>, UndirectedWithAux<G>, CCH, E>
where
    T: Send + Sync,
    E: Send + Sync,
    F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T], &mut [E], &mut [E]),
    G: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T], &mut [E], &mut [E]),
{
    pub fn new_undirected_with_aux(cch: &'a CCH, customize_cell: F, customize_separator: G) -> Self {
        Self::new_with_aux(cch, UndirectedWithAux(customize_cell), UndirectedWithAux(customize_separator))
    }
}

impl<'a, T, F, G, C> SeperatorBasedParallelCustomization<'a, T, F, G, C>
where
    T: Send + Sync,
    F: SubgraphCustomization<T, ()>,
    G: SubgraphCustomization<T, ()>,
    C: CCHT + Sync,
{
    /// Execute customization. Takes a mut slice to the full memory where weights that should be customized are stored.
    /// The setup callback can be used to perform additional scoped setup work.
    /// It has to call the callback that gets passed to it in turn.
    /// Otherwise nothing will happen.
    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T], setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync) {
        let mut up_aux = vec![(); upward.len()];
        let mut down_aux = vec![(); downward.len()];
        self.customize_with_aux(upward, downward, &mut up_aux, &mut down_aux, setup)
    }
}

impl<'a, T, F, G, C, E> SeperatorBasedParallelCustomization<'a, T, F, G, C, E>
where
    T: Send + Sync,
    E: Send + Sync,
    F: SubgraphCustomization<T, E>,
    G: SubgraphCustomization<T, E>,
    C: CCHT + Sync,
{
    pub fn new_with_aux(cch: &'a C, customize_cell: F, customize_separator: G) -> Self {
        if !cfg!(feature = "cch-disable-par") {
            cch.separators().validate_for_parallelization();
        }

        Self {
            cch,
            customize_cell,
            customize_separator,
            _t: std::marker::PhantomData::<T>,
            _e: std::marker::PhantomData::<E>,
        }
    }

    pub fn customize_with_aux(
        &self,
        upward: &'a mut [T],
        downward: &'a mut [T],
        up_aux: &'a mut [E],
        down_aux: &'a mut [E],
        setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync,
    ) {
        if cfg!(feature = "cch-disable-par") {
            setup(Box::new(|| {
                self.customize_cell
                    .exec(0..self.cch.forward_first_out().len() - 1, 0, 0, upward, downward, up_aux, down_aux)
            }));
        } else {
            let available_cpus = affinity::get_thread_affinity().unwrap();
            rayon::ThreadPoolBuilder::new()
                .build_scoped(
                    |thread| {
                        affinity::set_thread_affinity(&[available_cpus[thread.index()]]).unwrap();
                        setup(Box::new(|| thread.run()));
                    },
                    |pool| pool.install(|| self.customize_tree(self.cch.separators(), 0, upward, downward, up_aux, down_aux)),
                )
                .unwrap();
        }
    }

    fn customize_tree(&self, sep_tree: &SeparatorTree, offset: usize, upward: &'a mut [T], downward: &'a mut [T], up_aux: &'a mut [E], down_aux: &'a mut [E]) {
        let n = self.cch.forward_first_out().len() - 1;
        let forward_edge_offset = self.cch.forward_first_out()[offset] as usize;
        let backward_edge_offset = self.cch.backward_first_out()[offset] as usize;

        if sep_tree.num_nodes < n / (32 * rayon::current_num_threads()) {
            // if the current cell is small enough (load balancing parameters) run the customize_cell routine on it
            self.customize_cell.exec(
                offset..offset + sep_tree.num_nodes,
                forward_edge_offset,
                backward_edge_offset,
                upward,
                downward,
                up_aux,
                down_aux,
            );
        } else {
            // if not, split at the separator, process all subcells independently in parallel and the separator afterwards
            let mut sub_offset = offset;
            let mut sub_forward_edge_offset = forward_edge_offset;
            let mut sub_backward_edge_offset = backward_edge_offset;
            let mut sub_upward = &mut *upward;
            let mut sub_downward = &mut *downward;
            let mut sub_aux_up = &mut *up_aux;
            let mut sub_aux_down = &mut *down_aux;

            rayon::scope(|s| {
                for sub in &sep_tree.children {
                    let (this_sub_up, rest_up) =
                        (move || sub_upward)().split_at_mut(self.cch.forward_first_out()[sub_offset + sub.num_nodes] as usize - sub_forward_edge_offset);
                    let (this_sub_down, rest_down) =
                        (move || sub_downward)().split_at_mut(self.cch.backward_first_out()[sub_offset + sub.num_nodes] as usize - sub_backward_edge_offset);
                    let (this_aux_up, rest_aux_up) =
                        (move || sub_aux_up)().split_at_mut(self.cch.forward_first_out()[sub_offset + sub.num_nodes] as usize - sub_forward_edge_offset);
                    let (this_aux_down, rest_aux_down) =
                        (move || sub_aux_down)().split_at_mut(self.cch.backward_first_out()[sub_offset + sub.num_nodes] as usize - sub_backward_edge_offset);

                    sub_forward_edge_offset += this_sub_up.len();
                    sub_backward_edge_offset += this_sub_down.len();
                    // if the subcell is small enough don't bother spawning a thread
                    // this catches the case of very small cell at high levels which may sometime occur
                    // subcells are ordered descending by their size, so we will always first spawn of tasks for the big ones
                    if sub.num_nodes < n / (32 * rayon::current_num_threads()) {
                        self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down, this_aux_up, this_aux_down);
                    } else {
                        s.spawn(move |_| self.customize_tree(sub, sub_offset, this_sub_up, this_sub_down, this_aux_up, this_aux_down));
                    }
                    sub_offset += sub.num_nodes;
                    sub_upward = rest_up;
                    sub_downward = rest_down;
                    sub_aux_up = rest_aux_up;
                    sub_aux_down = rest_aux_down;
                }
            });

            // once all subcells are processed, process the separator itself
            self.customize_separator.exec(
                sub_offset..offset + sep_tree.num_nodes,
                forward_edge_offset,
                backward_edge_offset,
                upward,
                downward,
                up_aux,
                down_aux,
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

pub trait SubgraphPerfectCustomization<T, E>: Sync {
    fn exec(&self, nodes: Range<usize>, edges_up: *mut T, edges_down: *mut T, aux_up: *mut E, aux_down: *mut E);
}

pub struct NoAux<F>(F);
impl<F: Sync + Fn(Range<usize>, *mut T, *mut T), T, E> SubgraphPerfectCustomization<T, E> for NoAux<F> {
    fn exec(&self, nodes: Range<usize>, edges_up: *mut T, edges_down: *mut T, _aux_up: *mut E, _aux_down: *mut E) {
        (self.0)(nodes, edges_up, edges_down)
    }
}

impl<F: Sync + Fn(Range<usize>, *mut T, *mut T, *mut E, *mut E), T, E> SubgraphPerfectCustomization<T, E> for F {
    fn exec(&self, nodes: Range<usize>, edges_up: *mut T, edges_down: *mut T, aux_up: *mut E, aux_down: *mut E) {
        (self)(nodes, edges_up, edges_down, aux_up, aux_down)
    }
}

pub struct SeperatorBasedPerfectParallelCustomization<'a, T, F, G, C, E = ()> {
    cch: &'a C,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
    _e: std::marker::PhantomData<E>,
}

impl<'a, T, F, G, C> SeperatorBasedPerfectParallelCustomization<'a, T, NoAux<F>, NoAux<G>, C>
where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, *mut T, *mut T),
    G: Sync + Fn(Range<usize>, *mut T, *mut T),
    C: CCHT + Sync,
{
    pub fn new(cch: &'a C, customize_cell: F, customize_separator: G) -> Self {
        Self::new_with_aux(cch, NoAux(customize_cell), NoAux(customize_separator))
    }
}

impl<'a, T, F, G, C> SeperatorBasedPerfectParallelCustomization<'a, T, F, G, C>
where
    T: Send + Sync,
    F: SubgraphPerfectCustomization<T, ()>,
    G: SubgraphPerfectCustomization<T, ()>,
    C: CCHT + Sync,
{
    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T], setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync) {
        let mut up_aux = vec![(); upward.len()];
        let mut down_aux = vec![(); downward.len()];
        self.customize_with_aux(upward, downward, &mut up_aux, &mut down_aux, setup)
    }
}

/// Parallelization of perfect customization.
impl<'a, T, F, G, C, E> SeperatorBasedPerfectParallelCustomization<'a, T, F, G, C, E>
where
    T: Send + Sync,
    E: Send + Sync,
    F: SubgraphPerfectCustomization<T, E>,
    G: SubgraphPerfectCustomization<T, E>,
    C: CCHT + Sync,
{
    /// Setup for parallelization, we need a cch, and a routine for customization of cells
    /// and one for customization of separators.
    /// These should do the same thing in the end, but may achieve it in different ways because there are different performance trade-offs.
    /// The cell routine will be invoked several times in parallel and nodes will mostly have low degrees.
    /// The separator routine will be invoked only very few times in parallel, the final separator will be customized completely alone and nodes have high degrees.
    pub fn new_with_aux(cch: &'a C, customize_cell: F, customize_separator: G) -> Self {
        let separators = cch.separators();
        if cfg!(feature = "cch-disable-par") {
            separators.validate_for_parallelization();
        }

        Self {
            cch,
            customize_cell,
            customize_separator,
            _t: std::marker::PhantomData::<T>,
            _e: std::marker::PhantomData::<E>,
        }
    }

    /// Execute customization. Takes a mut slice to the full memory where weights that should be customized are stored.
    /// The setup callback can be used to perform additional scoped setup work.
    /// It has to call the callback that gets passed to it in turn.
    /// Otherwise nothing will happen.
    pub fn customize_with_aux(
        &self,
        upward: &'a mut [T],
        downward: &'a mut [T],
        up_aux: &'a mut [E],
        down_aux: &'a mut [E],
        setup: impl Fn(Box<dyn FnOnce() + '_>) + Sync,
    ) {
        if cfg!(feature = "cch-disable-par") {
            setup(Box::new(|| {
                self.customize_cell.exec(
                    0..self.cch.num_cch_nodes(),
                    upward.as_mut_ptr(),
                    downward.as_mut_ptr(),
                    up_aux.as_mut_ptr(),
                    down_aux.as_mut_ptr(),
                )
            }));
        } else {
            let available_cpus = affinity::get_thread_affinity().unwrap();
            rayon::ThreadPoolBuilder::new()
                .build_scoped(
                    |thread| {
                        affinity::set_thread_affinity(&[available_cpus[thread.index()]]).unwrap();
                        setup(Box::new(|| thread.run()));
                    },
                    |pool| {
                        pool.install(|| {
                            self.customize_tree(
                                self.cch.separators(),
                                self.cch.num_cch_nodes(),
                                upward.as_mut_ptr(),
                                downward.as_mut_ptr(),
                                up_aux.as_mut_ptr(),
                                down_aux.as_mut_ptr(),
                            )
                        })
                    },
                )
                .unwrap();
        }
    }

    fn customize_tree(&self, sep_tree: &SeparatorTree, offset: usize, upward: *mut T, downward: *mut T, up_aux: *mut E, down_aux: *mut E) {
        if sep_tree.num_nodes < self.cch.num_cch_nodes() / (32 * rayon::current_num_threads()) {
            // if the current cell is small enough (load balancing parameters) run the customize_cell routine on it
            self.customize_cell
                .exec(offset - sep_tree.num_nodes..offset, upward, downward, up_aux, down_aux);
        } else {
            // if not, process separator, then split into subcells and process them independently in parallel.
            let mut end_next = offset - sep_tree.nodes.len();
            self.customize_separator.exec(end_next..offset, upward, downward, up_aux, down_aux);
            let upward = PtrWrapper(upward);
            let downward = PtrWrapper(downward);
            let up_aux = PtrWrapper(up_aux);
            let down_aux = PtrWrapper(down_aux);

            rayon::scope(|s| {
                for sub in sep_tree.children.iter().rev() {
                    s.spawn(move |_| {
                        // force capturing the wrapper so closure is send/sync
                        let _ = &upward;
                        let _ = &downward;
                        let _ = &up_aux;
                        let _ = &down_aux;
                        self.customize_tree(sub, end_next, upward.0, downward.0, up_aux.0, down_aux.0)
                    });
                    end_next -= sub.num_nodes;
                }
            });
        }
    }
}
