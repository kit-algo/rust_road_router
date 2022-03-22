// WIP: CH potentials for TD Routing.

use rust_road_router::{
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    io::*,
    util::in_range_option::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(&arg);

    let graph = TDGraph::reconstruct_from(&path)?;

    let t_live: Timestamp = args.next().unwrap_or("0".to_string()).parse().unwrap();
    let live_data_file = args.next().unwrap_or("live_data".to_string());
    let mut live = vec![InRangeOption::NONE; graph.num_arcs()];
    let live_data = Vec::<(EdgeId, Weight, Weight)>::load_from(path.join(live_data_file))?;
    let max_t_soon = period();
    for (edge, weight, duration) in live_data {
        if duration < max_t_soon {
            live[edge as usize] = InRangeOption::some((weight, duration + t_live));
        }
    }

    let live_graph = PessimisticLiveTDGraph::new(graph, live);

    for edge_id in 0..live_graph.num_arcs() {
        if let Some(switching_start) = live_graph.live_starts_switching_at(edge_id as EdgeId) {
            let duration = std::cmp::max(switching_start, t_live) - t_live;
            println!("{duration}");
        }
    }

    Ok(())
}
