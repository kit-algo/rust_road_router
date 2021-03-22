//! A priority queue implemented with a 4-ary heap.
//!
//! Insertion and popping the minimal element have `O(log n)` time complexity.
//! Checking the minimal element is `O(1)`. Keys of elements in the heap can
//! also be increased or decreased
//!
//! # Examples
//!
//! ```
//! use rust_road_router::datastr::index_heap::{Indexing, IndexdMinHeap};
//!
//! #[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
//! pub struct State {
//!     pub distance: usize,
//!     pub node: usize,
//! }
//!
//!
//! // The `Indexing` traits needs to be implemented as well, so we can find elements to decrease their key.
//! impl Indexing for State {
//!     fn as_index(&self) -> usize {
//!         self.node as usize
//!     }
//! }
//!
//! let mut heap = IndexdMinHeap::new(3);
//! heap.push(State { node: 0, distance: 42 });
//! heap.push(State { node: 1, distance: 23 });
//! heap.push(State { node: 2, distance: 50000 });
//! assert_eq!(heap.peek().cloned(), Some(State { node: 1, distance: 23 }));
//! heap.decrease_key(State { node: 0, distance: 1 });
//! assert_eq!(heap.pop(), Some(State { node: 0, distance: 1 }));
//!
//! ```

use std::{cmp::min, mem::swap, ptr};

/// A trait to map elements in a heap to a unique index.
/// The element type of the `IndexdMinHeap` has to implement this trait.
pub trait Indexing {
    /// This method has to map a heap element to a unique `usize` index.
    fn as_index(&self) -> usize;
}

/// A priority queue where the elements are IDs from 0 to id_count-1 where id_count is a number that is set in the constructor.
/// The elements are sorted ascending by the ordering defined by the `Ord` trait.
/// The interface mirros the standard library BinaryHeap (except for the reversed order).
/// Only the methods necessary for dijkstras algorithm are implemented.
/// In addition, `increase_key` and `decrease_key` methods are available.
#[derive(Debug)]
pub struct IndexdMinHeap<T> {
    positions: Vec<usize>,
    data: Vec<T>,
}

const TREE_ARITY: usize = 4;
const INVALID_POSITION: usize = std::usize::MAX;

impl<T: Ord + Indexing> IndexdMinHeap<T> {
    /// Creates an empty `IndexdMinHeap` as a min-heap.
    /// The indices (as defined by the `Indexing` trait) of all inserted elements
    /// will have to be between in `[0, max_index)`
    pub fn new(max_id: usize) -> IndexdMinHeap<T> {
        IndexdMinHeap {
            positions: vec![INVALID_POSITION; max_id],
            data: Vec::new(),
        }
    }

    /// Returns the length of the binary heap.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks if the binary heap is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks if the heap already contains an element mapped to the given index
    pub fn contains_index(&self, id: usize) -> bool {
        self.positions[id] != INVALID_POSITION
    }

    pub fn get(&self, id: usize) -> Option<&T> {
        self.data.get(self.positions[id])
    }

    /// Returns an slice of all elements in no particular guaranteed order.
    pub fn elements(&self) -> &[T] {
        &self.data
    }

    /// Drops all items from the heap.
    pub fn clear(&mut self) {
        for element in &self.data {
            self.positions[element.as_index()] = INVALID_POSITION;
        }
        self.data.clear();
    }

    /// Returns a reference to the smallest item in the heap, or None if it is empty.
    #[allow(dead_code)]
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    /// Removes the greatest item from the binary heap and returns it, or None if it is empty.
    pub fn pop(&mut self) -> Option<T> {
        self.data.pop().map(|mut item| {
            self.positions[item.as_index()] = INVALID_POSITION;
            if !self.is_empty() {
                self.positions[item.as_index()] = 0;
                self.positions[self.data[0].as_index()] = INVALID_POSITION;
                swap(&mut item, &mut self.data[0]);
                self.move_down_in_tree(0);
            }
            item
        })
    }

    /// Pushes an item onto the binary heap if its not yet in the queue.
    pub fn push_unless_contained(&mut self, element: T) {
        if !self.contains_index(element.as_index()) {
            self.push(element)
        }
    }

    /// Pushes an item onto the binary heap.
    /// Panics if an element with the same index already exists.
    pub fn push(&mut self, element: T) {
        assert!(!self.contains_index(element.as_index()));
        let insert_position = self.len();
        self.positions[element.as_index()] = insert_position;
        self.data.push(element);
        self.move_up_in_tree(insert_position);
    }

    // Updates the key of an element.
    pub fn update_key(&mut self, element: T) {
        match element.cmp(&self.data[self.positions[element.as_index()]]) {
            std::cmp::Ordering::Less => self.decrease_key(element),
            std::cmp::Ordering::Greater => self.increase_key(element),
            _ => (),
        }
    }

    // Updates the key of an element if the new key is smaller than the old key.
    // Does nothing if the new key is larger.
    pub fn decrease_key(&mut self, element: T) {
        let position = self.positions[element.as_index()];
        self.data[position] = element;
        self.move_up_in_tree(position);
    }

    // Updates the key of an element if the new key is larger than the old key.
    // Does nothing if the new key is smaller.
    pub fn increase_key(&mut self, element: T) {
        let position = self.positions[element.as_index()];
        self.data[position] = element;
        self.move_down_in_tree(position);
    }

    fn move_up_in_tree(&mut self, mut position: usize) {
        while position > 0 {
            let parent = (position - 1) / TREE_ARITY;

            unsafe {
                if self.data.get_unchecked(parent) < self.data.get_unchecked(position) {
                    break;
                }

                self.positions
                    .swap(self.data.get_unchecked(parent).as_index(), self.data.get_unchecked(position).as_index());
                self.data.swap(parent, position);
            }
            position = parent;
        }
    }

    // fn move_up_in_tree(&mut self, position: usize) {
    //     unsafe {
    //         let mut hole = Hole::new(&mut self.data, position);

    //         while hole.pos > 0 {
    //             let parent = (hole.pos - 1) / TREE_ARITY;

    //             if hole.get(parent) < hole.element() {
    //                 break;
    //             }

    //             *self.positions.get_unchecked_mut(hole.get(parent).as_index()) = hole.pos;
    //             hole.move_to(parent);
    //         }

    //         *self.positions.get_unchecked_mut(hole.element().as_index()) = hole.pos;
    //     }
    // }

    // fn move_down_in_tree(&mut self, mut position: usize) {
    //     while let Some(smallest_child) = IndexdMinHeap::<T>::children_index_range(position, self.len()).min_by_key(|&child_index| unsafe { self.data.get_unchecked(child_index) }) {
    //         unsafe {
    //             if self.data.get_unchecked(smallest_child) >= self.data.get_unchecked(position) { return; } // no child is smaller

    //             self.positions.swap(self.data.get_unchecked(position).as_index(), self.data.get_unchecked(smallest_child).as_index());
    //             self.data.swap(smallest_child, position);
    //             position = smallest_child;
    //         }
    //     }
    // }

    fn move_down_in_tree(&mut self, position: usize) {
        unsafe {
            let heap_size = self.len();
            let mut hole = Hole::new(&mut self.data, position);

            loop {
                if let Some(smallest_child) = IndexdMinHeap::<T>::children_index_range(hole.pos, heap_size).min_by_key(|&child_index| hole.get(child_index)) {
                    if hole.get(smallest_child) >= hole.element() {
                        *self.positions.get_unchecked_mut(hole.element().as_index()) = hole.pos;
                        return; // no child is smaller
                    }

                    *self.positions.get_unchecked_mut(hole.get(smallest_child).as_index()) = hole.pos;
                    hole.move_to(smallest_child);
                } else {
                    *self.positions.get_unchecked_mut(hole.element().as_index()) = hole.pos;
                    return; // no children at all
                }
            }
        }
    }

    fn children_index_range(parent_index: usize, heap_size: usize) -> std::ops::Range<usize> {
        let first_child = TREE_ARITY * parent_index + 1;
        let last_child = min(TREE_ARITY * parent_index + TREE_ARITY + 1, heap_size);
        first_child..last_child
    }
}

// This is an optimization copied straight from the rust stdlib binary heap
// it allows to avoid always swapping elements pairwise and rather
// move each element only once.

/// Hole represents a hole in a slice i.e. an index without valid value
/// (because it was moved from or duplicated).
/// In drop, `Hole` will restore the slice by filling the hole
/// position with the value that was originally removed.
struct Hole<'a, T: 'a> {
    data: &'a mut [T],
    /// `elt` is always `Some` from new until drop.
    elt: Option<T>,
    pos: usize,
}

impl<'a, T> Hole<'a, T> {
    /// Create a new Hole at index `pos`.
    ///
    /// Unsafe because pos must be within the data slice.
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        debug_assert!(pos < data.len());
        let elt = ptr::read(&data[pos]);
        Hole { data, elt: Some(elt), pos }
    }

    /// Returns a reference to the element removed.
    #[inline]
    fn element(&self) -> &T {
        self.elt.as_ref().unwrap()
    }

    /// Returns a reference to the element at `index`.
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn get(&self, index: usize) -> &T {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        self.data.get_unchecked(index)
    }

    /// Move hole to new location
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        let index_ptr: *const _ = self.data.get_unchecked(index);
        let hole_ptr = self.data.get_unchecked_mut(self.pos);
        ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        self.pos = index;
    }
}

impl<'a, T> Drop for Hole<'a, T> {
    #[inline]
    fn drop(&mut self) {
        // fill the hole again
        unsafe {
            let pos = self.pos;
            ptr::write(self.data.get_unchecked_mut(pos), self.elt.take().unwrap());
        }
    }
}
