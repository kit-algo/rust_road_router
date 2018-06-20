use std::cmp::Ordering;

pub trait SortedSearchSliceExt {
    type Item;

    fn sorted_search(&self, x: &Self::Item) -> Result<usize, usize>
        where Self::Item: Ord
    {
        self.sorted_search_by(|p| p.cmp(x))
    }
    fn sorted_search_by<'a, F>(&'a self, f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> Ordering;

    fn sorted_search_by_key<'a, B, F>(&'a self, b: &B, mut f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> B, B: Ord
    {
        self.sorted_search_by(|k| f(k).cmp(b))
    }
}

impl<T> SortedSearchSliceExt for [T] {
    type Item = T;

    #[inline]
    fn sorted_search_by<'a, F>(&'a self, f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> Ordering {

        // if self.len() > 30 {
            self.binary_search_by(f)
        // } else {
        //     for (i, el) in self.iter().enumerate() {
        //         use std::cmp::Ordering::*;
        //         match f(el) {
        //             Equal => return Ok(i),
        //             Greater => return Err(i),
        //             Less => {}
        //         }
        //     }
        //     Err(self.len())
        // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sorted_search() {
        let s = [0, 1, 1, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55];

        assert_eq!(s.sorted_search(&13),  Ok(9));
        assert_eq!(s.sorted_search(&4),   Err(7));
        assert_eq!(s.sorted_search(&100), Err(13));
        let r = s.sorted_search(&1);
        assert!(match r { Ok(1...4) => true, _ => false, });
    }
}
