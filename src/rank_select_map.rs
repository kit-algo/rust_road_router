use std::mem::size_of;
use std::cmp::min;

#[derive(Debug)]
pub struct RankSelectMap {
    contained_keys_flags: Vec<bool>,
    prefix_sum: Vec<usize>
}
// 64 byte
const BITS_PER_PREFIX: usize = size_of::<usize>() * 8;

impl RankSelectMap {
    pub fn new(max_key: usize) -> RankSelectMap {
        RankSelectMap {
            contained_keys_flags: vec![false; max_key],
            prefix_sum: vec![0; max_key / BITS_PER_PREFIX + 2]
        }
    }

    pub fn len(&self) -> usize {
        match self.prefix_sum.last() {
            Some(&len) => len,
            None => 0,
        }
    }

    pub fn insert(&mut self, key: usize) {
        self.contained_keys_flags[key] = true;
    }

    pub fn compile(&mut self) {
        let mut previous = 0;
        for index in 1..self.prefix_sum.len() {
            self.prefix_sum[index] = self.bit_count_entire_range(index - 1) + previous;
            previous = self.prefix_sum[index];
        }
    }

    fn bit_count_entire_range(&self, range_index: usize) -> usize {
        let range = (range_index * BITS_PER_PREFIX)..min(self.contained_keys_flags.len(), (range_index + 1) * BITS_PER_PREFIX);
        self.contained_keys_flags[range].iter().filter(|x| **x).count()
    }

    pub fn at(&self, key: usize) -> usize {
        assert!(self.contained_keys_flags[key]);
        self.prefix_sum[key / BITS_PER_PREFIX] + self.bit_count_partial_range(key)
    }

    pub fn get(&self, key: usize) -> Option<usize> {
        if self.contained_keys_flags[key] {
            Some(self.prefix_sum[key / BITS_PER_PREFIX] + self.bit_count_partial_range(key))
        } else {
            None
        }
    }

    fn bit_count_partial_range(&self, key: usize) -> usize {
        let range = ((key / BITS_PER_PREFIX) * BITS_PER_PREFIX)..key;
        self.contained_keys_flags[range].iter().filter(|x| **x).count()
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
