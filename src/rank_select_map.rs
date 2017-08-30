use std::mem::size_of;
use std::cmp::{min, max};

use std::heap::{Heap, Alloc, Layout};

const CACHE_LINE_WIDTH: usize = 64; // bytes

const STORAGE_BITS: usize = size_of::<u64>() * 8;

#[derive(Debug)]
struct BitVec {
    data: Vec<u64>,
    size: usize
}

impl BitVec {
    fn new(size: usize) -> BitVec {
        let num_ints = (size + STORAGE_BITS - 1) / STORAGE_BITS;
        let data = unsafe {
            let pointer = Heap.alloc_zeroed(Layout::from_size_align(num_ints * size_of::<usize>(), CACHE_LINE_WIDTH).unwrap()).unwrap();
            Vec::from_raw_parts(pointer as *mut u64, num_ints, num_ints)
        };

        BitVec { data, size }
    }

    fn get(&self, index: usize) -> bool {
        assert!(index < self.size);
        self.data[index / STORAGE_BITS] & (1 << (index % STORAGE_BITS)) != 0
    }

    fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.size);
        if value {
            self.data[index / STORAGE_BITS] |= 1 << (index % STORAGE_BITS);
        } else {
            self.data[index / STORAGE_BITS] &= !(1 << (index % STORAGE_BITS));
        }
    }
}

#[derive(Debug)]
pub struct RankSelectMap {
    contained_keys_flags: BitVec,
    prefix_sum: Vec<usize>
}

const BITS_PER_PREFIX: usize = CACHE_LINE_WIDTH * 8;
const INTS_PER_PREFIX: usize = BITS_PER_PREFIX / STORAGE_BITS;

impl RankSelectMap {
    pub fn new(max_key: usize) -> RankSelectMap {
        RankSelectMap {
            contained_keys_flags: BitVec::new(max_key),
            prefix_sum: vec![0; (max_key + BITS_PER_PREFIX - 1) / BITS_PER_PREFIX + 1]
        }
    }

    pub fn len(&self) -> usize {
        match self.prefix_sum.last() {
            Some(&len) => len,
            None => 0,
        }
    }

    pub fn insert(&mut self, key: usize) {
        self.contained_keys_flags.set(key, true);
    }

    pub fn compile(&mut self) {
        let mut previous = 0;
        for index in 1..self.prefix_sum.len() {
            self.prefix_sum[index] = self.bit_count_entire_range(index - 1) + previous;
            previous = self.prefix_sum[index];
        }
    }

    fn bit_count_entire_range(&self, range_index: usize) -> usize {
        let range = (range_index * INTS_PER_PREFIX)..min((range_index + 1) * INTS_PER_PREFIX, self.contained_keys_flags.data.len());
        self.contained_keys_flags.data[range].iter().map(|num| num.count_ones() as usize).sum()
    }

    pub fn at(&self, key: usize) -> usize {
        assert!(self.contained_keys_flags.get(key));
        self.prefix_sum[key / BITS_PER_PREFIX] + self.bit_count_partial_range(key)
    }

    pub fn get(&self, key: usize) -> Option<usize> {
        if self.contained_keys_flags.get(key) {
            Some(self.prefix_sum[key / BITS_PER_PREFIX] + self.bit_count_partial_range(key))
        } else {
            None
        }
    }

    fn bit_count_partial_range(&self, key: usize) -> usize {
        let int = key / STORAGE_BITS;
        let range = ((int / INTS_PER_PREFIX) * INTS_PER_PREFIX)..(max(int, 1) - 1);
        let sum: usize = self.contained_keys_flags.data[range].iter().map(|num| num.count_ones() as usize).sum();
        let num = (self.contained_keys_flags.data[int] % (1 << (key % STORAGE_BITS))).count_ones() as usize;
        sum + num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut map = RankSelectMap::new(150);
        map.insert(31);
        map.insert(52);
        map.insert(2);
        map.insert(130);
        map.insert(0);
        map.insert(149);
        map.compile();

        assert_eq!(map.at(0), 0);
        assert_eq!(map.at(2), 1);
        assert_eq!(map.get(31), Some(2));
        assert_eq!(map.get(32), None);
        assert_eq!(map.at(52), 3);
        assert_eq!(map.at(130), 4);
        assert_eq!(map.at(149), 5);

        assert_eq!(map.len(), 6);
    }
}
