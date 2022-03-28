use rand::prelude::*;
use rust_road_router::{
    algo::dijkstra::{query::td_dijkstra::*, *},
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let tt_units_per_s = 1000;
    let mut dijk_data = DijkstraData::new(graph.num_nodes());

    let mut rng = StdRng::from_entropy();

    let num_queries = args.next().map(|arg| arg.parse().expect("could not parse num_queries")).unwrap_or(10000);

    let mut sources_1h = Vec::<NodeId>::with_capacity(num_queries);
    let mut targets_1h = Vec::<NodeId>::with_capacity(num_queries);
    let mut departures_1h = Vec::<NodeId>::with_capacity(num_queries);

    let mut ops = TDDijkstraOps::default();
    for _ in 0..num_queries {
        let from = rng.gen_range(0..n as NodeId);
        let at = rng.gen_range(0..period());
        let mut dijkstra = DijkstraRun::query(
            &graph,
            &mut dijk_data,
            &mut ops,
            DijkstraInit {
                source: NodeIdT(from),
                initial_state: at,
            },
        );

        while let Some(node) = dijkstra.next() {
            if *dijkstra.tentative_distance(node) - at > 3600 * tt_units_per_s {
                sources_1h.push(from);
                targets_1h.push(node);
                departures_1h.push(at);
                break;
            }
        }
    }

    sources_1h.write_to(&path.join("queries/1h/source"))?;
    targets_1h.write_to(&path.join("queries/1h/target"))?;
    departures_1h.write_to(&path.join("queries/1h/departure"))?;

    Ok(())
}
