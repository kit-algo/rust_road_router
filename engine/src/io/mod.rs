use std::{
    fs::{metadata, File},
    io::{prelude::*, Result},
    mem,
    path::Path,
    slice,
};

pub trait DataBytes {
    fn data_bytes(&self) -> &[u8];
}

pub trait DataBytesMut {
    fn data_bytes_mut(&mut self) -> &mut [u8];
}

impl<T: Copy> DataBytes for [T] {
    fn data_bytes(&self) -> &[u8] {
        let num_bytes = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, num_bytes) }
    }
}

impl<T: Copy> DataBytes for Vec<T> {
    fn data_bytes(&self) -> &[u8] {
        &self[..].data_bytes()
    }
}

impl<T: Copy> DataBytesMut for [T] {
    fn data_bytes_mut(&mut self) -> &mut [u8] {
        let num_bytes = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr() as *mut u8, num_bytes) }
    }
}

impl<T: Copy> DataBytesMut for Vec<T> {
    fn data_bytes_mut(&mut self) -> &mut [u8] {
        let num_bytes = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr() as *mut u8, num_bytes) }
    }
}

pub trait Store: DataBytes {
    fn write_to(&self, filename: &str) -> Result<()> {
        File::create(filename)?.write_all(self.data_bytes())
    }
}

impl<T: DataBytes> Store for T {}
impl<T> Store for [T] where [T]: DataBytes {}

pub trait Load: DataBytesMut + Sized {
    fn new_with_bytes(num_bytes: usize) -> Self;

    fn load_from<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let metadata = metadata(filename.as_ref())?;
        let mut file = File::open(filename)?;

        let mut object = Self::new_with_bytes(metadata.len() as usize);
        assert_eq!(metadata.len() as usize, object.data_bytes_mut().len());
        file.read_exact(object.data_bytes_mut())?;

        Ok(object)
    }
}

impl<T: Default + Copy> Load for Vec<T> {
    fn new_with_bytes(num_bytes: usize) -> Self {
        assert_eq!(num_bytes % mem::size_of::<T>(), 0);
        let num_elements = num_bytes / mem::size_of::<T>();
        (0..num_elements).map(|_| T::default()).collect()
    }
}

pub trait Deconstruct: Sized {
    fn store_each(&self, store_callback: &dyn Fn(&str, &dyn Store) -> Result<()>) -> Result<()>;

    fn deconstruct_to(&self, dir: &str) -> Result<()> {
        let path = Path::new(dir);

        self.store_each(&|name, object: &dyn Store| object.write_to(path.join(name).to_str().unwrap()))
    }
}

#[derive(Debug)]
pub struct Loader<'a> {
    path: &'a Path,
}

impl<'a> Loader<'a> {
    pub fn load<T: Load, P: AsRef<Path>>(&self, filename: P) -> Result<T> {
        T::load_from(self.path.join(filename))
    }
}

pub trait Reconstruct: Sized {
    fn reconstruct_with(loader: Loader) -> Result<Self>;

    fn reconstruct_from(dir: &str) -> Result<Self> {
        let path = Path::new(dir);
        Self::reconstruct_with(Loader { path })
    }
}

pub trait ReconstructPrepared<T: Sized>: Sized {
    fn reconstruct_with(self, loader: Loader) -> Result<T>;

    fn reconstruct_from(self, dir: &str) -> Result<T> {
        let path = Path::new(dir);
        self.reconstruct_with(Loader { path })
    }
}
