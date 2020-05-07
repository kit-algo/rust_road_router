/// Number of dijkstra queries performed for experiments.
/// Can be overriden through the NUM_DIJKSTRA_QUERIES env var.
#[cfg(not(override_num_dijkstra_queries))]
pub const NUM_DIJKSTRA_QUERIES: usize = 1000;
#[cfg(override_num_dijkstra_queries)]
pub const NUM_DIJKSTRA_QUERIES: usize = include!(concat!(env!("OUT_DIR"), "/NUM_DIJKSTRA_QUERIES"));

pub mod chpot;
