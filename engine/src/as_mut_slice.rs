pub trait AsMutSlice<T> {
    fn as_mut_slice(&mut self) -> &mut [T];
}

impl<V> AsMutSlice<V> for Vec<V> {
    fn as_mut_slice(&mut self) -> &mut [V] {
        &mut self[..]
    }
}
