#![feature(compiler_fences)]
#![feature(const_fn)]
#![feature(allocator_api)]

extern crate nav_types;
extern crate postgres;

pub mod shortest_path;
pub mod graph;
pub mod io;
mod index_heap;
pub mod rank_select_map;
mod import;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}


