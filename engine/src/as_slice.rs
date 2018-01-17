use std::sync::Arc;

pub trait AsSlice<T> {
    fn as_slice(&self) -> &[T];
}

impl<V> AsSlice<V> for Vec<V> where
{
    fn as_slice(&self) -> &[V] {
        &self[..]
    }
}

impl<'a, V> AsSlice<V> for &'a [V] where
{
    fn as_slice(&self) -> &[V] {
        self
    }
}

impl<V> AsSlice<V> for Arc<Vec<V>> where
{
    fn as_slice(&self) -> &[V] {
        &self[..]
    }
}
