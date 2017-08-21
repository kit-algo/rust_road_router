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
            positions: vec![INVALID_POSITION, max_id],
            data: Vec::new()
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_index(&self, id: usize) -> bool {
        self.positions[id] != INVALID_POSITION
    }

    pub fn clear(&mut self) {
        for element in self.data.iter() {
            self.positions[element.as_index()] = INVALID_POSITION;
        }
        self.data.clear();
    }

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
        assert!(element.as_index() < self.positions.len());
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
        assert!(element.as_index() < self.positions.len());
        let position = self.positions[element.as_index()];
        assert!(element < self.data[position]);

        self.data[position] = element;
        self.move_up_in_tree(position);
    }

    // Updates the key of an element if the new key is larger than the old key.
    // Does nothing if the new key is smaller.
    // Undefined if the element is not part of the queue.
    pub fn increase_key(&mut self, element: T) {
        assert!(element.as_index() < self.positions.len());
        let position = self.positions[element.as_index()];
        assert!(element > self.data[position]);

        self.data[position] = element;
        self.move_down_in_tree(position);
    }

    fn move_up_in_tree(&mut self, position: usize) {
        let mut position = position;
        while position != 0 {
            let parent = (position - 1) / TREE_ARITY;
            if self.data[parent] > self.data[position] {
                self.data.swap(position, parent);
                self.positions.swap(self.data[position].as_index(), self.data[parent].as_index());
            }
            position = parent;
        }
    }

    fn move_down_in_tree(&mut self, position: usize) {
        let mut position = position;

        loop {
            let first_child = TREE_ARITY * position + 1;
            if first_child >= self.len() {
                return; // no children
            }

            let mut smallest_child = first_child;
            for child in (first_child + 1)..min(TREE_ARITY * position + TREE_ARITY + 1, self.len()) {
                if self.data[smallest_child] > self.data[child] {
                    smallest_child = child;
                }
            }

            if self.data[smallest_child] >= self.data[position] {
                return; // no child is smaller
            }


            self.data.swap(position, smallest_child);
            self.positions.swap(self.data[position].as_index(), self.data[smallest_child].as_index());
            position = smallest_child;
        }

    }
}
