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
    let mut dijk_data = DijkstraData::new(graph.num_nodes());

    let mut rng = StdRng::from_entropy();

    let num_queries = args.next().map(|arg| arg.parse().expect("could not parse num_queries")).unwrap_or(1000);

    let mut rank_sources = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut rank_targets = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut rank_departures = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut rank_ranks = Vec::<u32>::with_capacity(30 * num_queries);

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

        let mut i: usize = 0;
        while let Some(node) = dijkstra.next() {
            i += 1;
            if (i & (i - 1)) == 0 {
                rank_sources.push(from);
                rank_targets.push(node);
                rank_departures.push(at);
                rank_ranks.push(i.trailing_zeros());
            }
        }
    }

    let mut perm: Vec<_> = (0..rank_sources.len()).collect();
    perm.shuffle(&mut rng);
    let mut shuffled_rank_sources = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut shuffled_rank_targets = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut shuffled_rank_ranks = Vec::<u32>::with_capacity(30 * num_queries);
    let mut shuffled_rank_departures = Vec::<u32>::with_capacity(30 * num_queries);
    for idx in perm {
        shuffled_rank_sources.push(rank_sources[idx]);
        shuffled_rank_targets.push(rank_targets[idx]);
        shuffled_rank_ranks.push(rank_ranks[idx]);
        shuffled_rank_departures.push(rank_departures[idx]);
    }

    shuffled_rank_sources.write_to(&path.join("queries/rank/source"))?;
    shuffled_rank_targets.write_to(&path.join("queries/rank/target"))?;
    shuffled_rank_ranks.write_to(&path.join("queries/rank/rank"))?;
    shuffled_rank_departures.write_to(&path.join("queries/rank/departure"))?;

    Ok(())
}
