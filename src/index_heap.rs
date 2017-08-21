use std;
use std::cmp::min;
use std::mem::swap;

pub trait Indexing {
    fn as_index(&self) -> usize;
}

// A priority queue where the elements are IDs from 0 to id_count-1 where id_count is a number that is set in the constructor.
// The elements are sorted by integer keys.
#[derive(Debug)]
pub struct IndexdMinHeap<T: Ord + Indexing> {
    positions: Vec<usize>,
    data: Vec<T>
}

const TREE_ARITY: usize = 4;
const INVALID_POSITION: usize = std::usize::MAX;

impl<T: Ord + Indexing> IndexdMinHeap<T> {
    pub fn new(max_id: usize) -> IndexdMinHeap<T> {
        IndexdMinHeap {
            positions: vec![INVALID_POSITION; max_id],
            data: Vec::new()
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_index(&self, id: usize) -> bool {
        self.positions[id] != INVALID_POSITION
    }

    pub fn clear(&mut self) {
        for element in self.data.iter() {
            self.positions[element.as_index()] = INVALID_POSITION;
        }
        self.data.clear();
    }

    #[allow(dead_code)]
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

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

    pub fn push(&mut self, element: T) {
        assert!(!self.contains_index(element.as_index()));
        let insert_position = self.len();
        self.positions[element.as_index()] = insert_position;
        self.data.push(element);
        self.move_up_in_tree(insert_position);
    }

    // Updates the key of an element if the new key is smaller than the old key.
    // Does nothing if the new key is larger.
    // Undefined if the element is not part of the queue.
    pub fn decrease_key(&mut self, element: T) {
        let position = self.positions[element.as_index()];
        self.data[position] = element;
        self.move_up_in_tree(position);
    }

    // Updates the key of an element if the new key is larger than the old key.
    // Does nothing if the new key is smaller.
    // Undefined if the element is not part of the queue.
    #[allow(dead_code)]
    pub fn increase_key(&mut self, element: T) {
        let position = self.positions[element.as_index()];
        self.data[position] = element;
        self.move_down_in_tree(position);
    }

    fn move_up_in_tree(&mut self, position: usize) {
        let mut position = position;
        while position != 0 {
            let parent = (position - 1) / TREE_ARITY;
            if self.data[parent] > self.data[position] {
                self.swap(parent, position);
            }
            position = parent;
        }
    }

    fn move_down_in_tree(&mut self, position: usize) {
        let mut position = position;

        loop {
            if let Some(smallest_child) = self.children_index_range(position).min_by_key(|&child_index| &self.data[child_index]) {
                if self.data[smallest_child] >= self.data[position] {
                    return; // no child is smaller
                }

                self.swap(smallest_child, position);
                position = smallest_child;
            } else {
                return; // no children at all
            }
        }
    }

    fn swap(&mut self, first_index: usize, second_index: usize) {
        self.data.swap(first_index, second_index);
        self.positions.swap(self.data[first_index].as_index(), self.data[second_index].as_index());
    }

    fn children_index_range(&self, parent_index: usize) -> std::ops::Range<usize> {
        let first_child = TREE_ARITY * parent_index + 1;
        let last_child = min(TREE_ARITY * parent_index + TREE_ARITY + 1, self.len());
        first_child..last_child
    }
}
