use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::Result;
use std::mem;

pub fn read_into_vector<T>(filename: &str) -> Result<Vec<T>> {
    let metadata = fs::metadata(filename)?;
    let bytes = metadata.len() as usize;
    let num_elements = bytes / mem::size_of::<T>();
    let mut file = File::open(filename)?;

    let mut buffer = Vec::with_capacity(bytes);
    file.read_to_end(&mut buffer)?;

    let p = buffer.as_mut_ptr();
    let buffer = unsafe {
        mem::forget(buffer);
        Vec::<T>::from_raw_parts(p as *mut T, num_elements, num_elements)
    };

    Ok(buffer)
}
