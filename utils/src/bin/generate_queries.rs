use rand::prelude::*;
use rust_road_router::{algo::dijkstra::*, cli::CliErr, datastr::graph::*, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0];

    let mut dijk_data = DijkstraData::new(graph.num_nodes());

    let mut rng = StdRng::from_entropy();

    let num_queries = 1000;
    let uniform_random_sources: Vec<_> = std::iter::repeat_with(|| rng.gen_range(0..graph.num_nodes() as NodeId))
        .take(num_queries)
        .collect();
    let uniform_random_targets: Vec<_> = std::iter::repeat_with(|| rng.gen_range(0..graph.num_nodes() as NodeId))
        .take(num_queries)
        .collect();

    let num_queries = 1000;

    let mut rank_sources = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut rank_targets = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut rank_ranks = Vec::<u32>::with_capacity(30 * num_queries);

    let mut sources_1h = Vec::<NodeId>::with_capacity(num_queries);
    let mut targets_1h = Vec::<NodeId>::with_capacity(num_queries);

    let mut ops = DefaultOps::default();
    for _ in 0..num_queries {
        let from = rng.gen_range(0..graph.num_nodes() as NodeId);
        let mut dijkstra = DijkstraRun::query(&graph, &mut dijk_data, &mut ops, DijkstraInit::from(from));

        let mut i: usize = 0;
        let mut tt_greater_1h = false;
        while let Some(node) = dijkstra.next() {
            i += 1;
            if (i & (i - 1)) == 0 {
                rank_sources.push(from);
                rank_targets.push(node);
                rank_ranks.push(i.trailing_zeros());
            }
            if !tt_greater_1h && *dijkstra.tentative_distance(node) > 3600 * tt_units_per_s {
                tt_greater_1h = true;
                sources_1h.push(from);
                targets_1h.push(node);
            }
        }
    }

    let mut perm: Vec<_> = (0..rank_sources.len()).collect();
    perm.shuffle(&mut rng);
    let mut shuffled_rank_sources = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut shuffled_rank_targets = Vec::<NodeId>::with_capacity(30 * num_queries);
    let mut shuffled_rank_ranks = Vec::<u32>::with_capacity(30 * num_queries);
    for idx in perm {
        shuffled_rank_sources.push(rank_sources[idx]);
        shuffled_rank_targets.push(rank_targets[idx]);
        shuffled_rank_ranks.push(rank_ranks[idx]);
    }

    uniform_random_sources.write_to(&path.join("queries/uniform/source"))?;
    uniform_random_targets.write_to(&path.join("queries/uniform/target"))?;
    shuffled_rank_sources.write_to(&path.join("queries/rank/source"))?;
    shuffled_rank_targets.write_to(&path.join("queries/rank/target"))?;
    shuffled_rank_ranks.write_to(&path.join("queries/rank/rank"))?;
    sources_1h.write_to(&path.join("queries/1h/source"))?;
    targets_1h.write_to(&path.join("queries/1h/target"))?;

    Ok(())
}
