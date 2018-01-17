use std::borrow::Borrow;

pub trait AsSlice<T> {
    fn as_slice(&self) -> &[T];
}

impl<T, V> AsSlice<V> for T where
    T: Borrow<[V]>,
{
    fn as_slice(&self) -> &[V] {
        self.borrow()
    }
}
