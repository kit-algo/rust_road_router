#![feature(compiler_fences)]
#![feature(const_fn)]
#![feature(allocator_api)]
#![feature(const_size_of)]
#![feature(mpsc_select)]
#![feature(conservative_impl_trait)]

extern crate nav_types;
extern crate postgres;
extern crate csv;
extern crate glob;

pub mod shortest_path;
pub mod graph;
pub mod io;
mod index_heap;
pub mod rank_select_map;
pub mod import;
pub mod link_speed_estimates;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}


