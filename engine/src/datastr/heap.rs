use core::mem::ManuallyDrop;
use core::ptr;
// use std::cmp::Ordering;

#[derive(Clone)]
pub struct Heap<T> {
    data: Vec<T>,
    split: usize,
    // cmp: C,
}

// impl<T, C: FnMut(&T, &T) -> Ordering> Heap<T, C> {
impl<T> Heap<T> {
    pub const fn new() -> Self {
        // pub fn new(cmp: C) -> Self {
        Self { data: vec![], split: 0 }
    }
}

#[allow(unused_unsafe)]
impl<T: Ord> Heap<T> {
    pub fn pop(&mut self) -> Option<&T> {
        match self.split {
            0 => return None,
            1 => self.split = 0,
            _ => {
                self.split -= 1;
                self.data.swap(0, self.split);
                // SAFETY: !self.is_empty() means that self.len() > 0
                unsafe { self.sift_down_to_bottom(0) };
            }
        }
        Some(&self.data[self.split])
    }

    pub fn push(&mut self, item: T) {
        let old_len = self.split;
        self.data.push(item);
        let pushed_idx = self.data.len() - 1;
        self.data.swap(old_len, pushed_idx);
        self.split += 1;
        // SAFETY: Since we pushed a new item it means that
        //  old_len = self.len() - 1 < self.len()
        unsafe { self.sift_up(0, old_len) };
    }

    // The implementations of sift_up and sift_down use unsafe blocks in
    // order to move an element out of the vector (leaving behind a
    // hole), shift along the others and move the removed element back into the
    // vector at the final location of the hole.
    // The `Hole` type is used to represent this, and make sure
    // the hole is filled back at the end of its scope, even on panic.
    // Using a hole reduces the constant factor compared to using swaps,
    // which involves twice as many moves.

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    unsafe fn sift_up(&mut self, start: usize, pos: usize) -> usize {
        // Take out the value at `pos` and create a hole.
        // SAFETY: The caller guarantees that pos < self.len()
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };

        while hole.pos() > start {
            let parent = (hole.pos() - 1) / 2;

            // SAFETY: hole.pos() > start >= 0, which means hole.pos() > 0
            //  and so hole.pos() - 1 can't underflow.
            //  This guarantees that parent < hole.pos() so
            //  it's a valid index and also != hole.pos().
            if hole.element() <= unsafe { hole.get(parent) } {
                // if !matches!((self.cmp)(hole.element(), unsafe { hole.get(parent) }), Ordering::Less) {
                break;
            }

            // SAFETY: Same as above
            unsafe { hole.move_to(parent) };
        }

        hole.pos()
    }

    /// Take an element at `pos` and move it down the heap,
    /// while its children are larger.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < end <= self.len()`.
    unsafe fn sift_down_range(&mut self, pos: usize, end: usize) {
        // SAFETY: The caller guarantees that pos < end <= self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // compare with the greater of the two children
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += unsafe { hole.get(child) <= hole.get(child + 1) } as usize;
            // child += unsafe { !matches!((self.cmp)(hole.get(child), hole.get(child + 1)), Ordering::Less) } as usize;

            // if we are already in order, stop.
            // SAFETY: child is now either the old child or the old child+1
            //  We already proven that both are < self.len() and != hole.pos()
            if hole.element() >= unsafe { hole.get(child) } {
                // if !matches!((self.cmp)(hole.element(), unsafe { hole.get(child) }), Ordering::Greater) {
                return;
            }

            // SAFETY: same as above.
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        // SAFETY: && short circuit, which means that in the
        //  second condition it's already true that child == end - 1 < self.len().
        if child == end - 1 && hole.element() < unsafe { hole.get(child) } {
            // if child == end - 1 && matches!((self.cmp)(hole.element(), unsafe { hole.get(child) }), Ordering::Greater) {
            // SAFETY: child is already proven to be a valid index and
            //  child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }
    }

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    unsafe fn sift_down(&mut self, pos: usize) {
        let len = self.split;
        // SAFETY: pos < len is guaranteed by the caller and
        //  obviously len = self.len() <= self.len().
        unsafe { self.sift_down_range(pos, len) };
    }

    /// Take an element at `pos` and move it all the way down the heap,
    /// then sift it up to its position.
    ///
    /// Note: This is faster when the element is known to be large / should
    /// be closer to the bottom.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    unsafe fn sift_down_to_bottom(&mut self, mut pos: usize) {
        let end = self.split;
        let start = pos;

        // SAFETY: The caller guarantees that pos < self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += unsafe { hole.get(child) <= hole.get(child + 1) } as usize;
            // child += unsafe { !matches!((self.cmp)(hole.get(child), hole.get(child + 1)), Ordering::Less) } as usize;

            // SAFETY: Same as above
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        if child == end - 1 {
            // SAFETY: child == end - 1 < self.len(), so it's a valid index
            //  and child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }
        pos = hole.pos();
        drop(hole);

        // SAFETY: pos is the position in the hole and was already proven
        //  to be a valid index.
        unsafe { self.sift_up(start, pos) };
    }

    /// Rebuild assuming data[0..start] is still a proper heap.
    fn rebuild_tail(&mut self, start: usize) {
        if start == self.split {
            return;
        }

        let tail_len = self.split - start;

        #[inline(always)]
        fn log2_fast(x: usize) -> usize {
            (usize::BITS - x.leading_zeros() - 1) as usize
        }

        // `rebuild` takes O(self.len()) operations
        // and about 2 * self.len() comparisons in the worst case
        // while repeating `sift_up` takes O(tail_len * log(start)) operations
        // and about 1 * tail_len * log_2(start) comparisons in the worst case,
        // assuming start >= tail_len. For larger heaps, the crossover point
        // no longer follows this reasoning and was determined empirically.
        let better_to_rebuild = if start < tail_len {
            true
        } else if self.split <= 2048 {
            2 * self.split < tail_len * log2_fast(start)
        } else {
            2 * self.split < tail_len * 11
        };

        if better_to_rebuild {
            self.rebuild();
        } else {
            for i in start..self.split {
                // SAFETY: The index `i` is always less than self.len().
                unsafe { self.sift_up(0, i) };
            }
        }
    }

    fn rebuild(&mut self) {
        let mut n = self.split / 2;
        while n > 0 {
            n -= 1;
            // SAFETY: n starts from self.len() / 2 and goes down to 0.
            //  The only case when !(n < self.len()) is if
            //  self.len() == 0, but it's ruled out by the loop condition.
            unsafe { self.sift_down(n) };
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(&e)` returns
    /// `false`. The elements are visited in unsorted (and unspecified) order.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// #![feature(binary_heap_retain)]
    /// use std::collections::BinaryHeap;
    ///
    /// let mut heap = BinaryHeap::from(vec![-10, -5, 1, 2, 4, 13]);
    ///
    /// heap.retain(|x| x % 2 == 0); // only keep even numbers
    ///
    /// assert_eq!(heap.into_sorted_vec(), [-10, 2, 4])
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        let mut first_removed = self.split;
        let mut i = 0;
        let mut removed_from_heap = 0;
        self.data.retain(|e| {
            let keep = f(e);
            if !keep && i < self.split {
                removed_from_heap += 1;
            }
            if !keep && i < first_removed {
                first_removed = i;
            }
            i += 1;
            keep
        });
        self.split -= removed_from_heap;
        // data[0..first_removed] is untouched, so we only need to rebuild the tail:
        self.rebuild_tail(first_removed);
    }

    pub fn peek(&self) -> Option<&T> {
        if self.split > 0 {
            self.data.get(0)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.split = 0;
    }

    pub fn popped(&self) -> &[T] {
        &self.data[self.split..]
    }
}

/// Hole represents a hole in a slice i.e., an index without valid value
/// (because it was moved from or duplicated).
/// In drop, `Hole` will restore the slice by filling the hole
/// position with the value that was originally removed.
struct Hole<'a, T: 'a> {
    data: &'a mut [T],
    elt: ManuallyDrop<T>,
    pos: usize,
}

#[allow(unused_unsafe)]
impl<'a, T> Hole<'a, T> {
    /// Create a new `Hole` at index `pos`.
    ///
    /// Unsafe because pos must be within the data slice.
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        debug_assert!(pos < data.len());
        // SAFE: pos should be inside the slice
        let elt = unsafe { ptr::read(data.get_unchecked(pos)) };
        Hole {
            data,
            elt: ManuallyDrop::new(elt),
            pos,
        }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    /// Returns a reference to the element removed.
    #[inline]
    fn element(&self) -> &T {
        &self.elt
    }

    /// Returns a reference to the element at `index`.
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn get(&self, index: usize) -> &T {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe { self.data.get_unchecked(index) }
    }

    /// Move hole to new location
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe {
            let ptr = self.data.as_mut_ptr();
            let index_ptr: *const _ = ptr.add(index);
            let hole_ptr = ptr.add(self.pos);
            ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        }
        self.pos = index;
    }
}

impl<T> Drop for Hole<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // fill the hole again
        unsafe {
            let pos = self.pos;
            ptr::copy_nonoverlapping(&*self.elt, self.data.get_unchecked_mut(pos), 1);
        }
    }
}
