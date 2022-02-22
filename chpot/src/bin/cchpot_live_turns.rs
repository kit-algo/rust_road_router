use rust_road_router::{
    algo::{
        a_star::*,
        ch_potentials::{query::BiDirServer as BiDirTopo, *},
        customizable_contraction_hierarchy::*,
        dijkstra::{query::bidirectional_dijkstra::Server as DijkServer, *},
    },
    cli::CliErr,
    datastr::graph::*,
    datastr::node_order::*,
    experiments,
    io::*,
    report::*,
};

use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("bidir_cchpot_turns");

    let mut rng = experiments::rng(Default::default());

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let mut graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let live_travel_time = Vec::<Weight>::load_from(path.join("live_travel_time"))?;
    for (query, input) in graph.weights_mut().iter_mut().zip(live_travel_time.iter()) {
        if input > query {
            *query = *input;
        }
    }

    let forbidden_turn_from_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_from_arc"))?;
    let forbidden_turn_to_arc = Vec::<EdgeId>::load_from(path.join("forbidden_turn_to_arc"))?;

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    let mut iter = forbidden_turn_from_arc.iter().zip(forbidden_turn_to_arc.iter()).peekable();

    let exp_graph = line_graph(&graph, |edge1_idx, edge2_idx| {
        while let Some((&from_arc, &to_arc)) = iter.peek() {
            if from_arc < edge1_idx || (from_arc == edge1_idx && to_arc < edge2_idx) {
                iter.next();
            } else {
                break;
            }
        }

        if iter.peek() == Some(&(&edge1_idx, &edge2_idx)) {
            return None;
        }
        if tail[edge1_idx as usize] == graph.head()[edge2_idx as usize] {
            return None;
        }
        Some(0)
    });

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    affinity::set_thread_affinity(&[0]).unwrap();

    let pots = (
        TurnExpandedPotential::new(&graph, cch_pot_data.forward_potential()),
        // since this is the reverse pot, head is the tail array of the reversed turn expanded graph
        TurnExpandedPotential::new_with_tail(graph.head().to_vec(), cch_pot_data.backward_potential()),
    );

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore = BiDirTopo::<_, AlternatingDirs>::new(&exp_graph, SymmetricBiDirPotential::<_, _>::new(pots.0, pots.1));
    drop(virtual_topocore_ctxt);

    let n = exp_graph.num_nodes();
    experiments::run_random_queries_with_callbacks(
        n,
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _| (),
        // |mut res| {
        //     report!(
        //         "num_pot_computations",
        //         res.as_mut().map(|res| res.data().potential().inner().num_pot_computations()).unwrap_or(0)
        //     );
        //     report!(
        //         "lower_bound",
        //         res.as_mut()
        //             .map(|res| {
        //                 let from = res.data().query().from();
        //                 res.data().lower_bound(from)
        //             })
        //             .flatten()
        //     );
        // },
        |_, _| None,
    );

    let mut server = DijkServer::<_, _, _, AlternatingDirs>::new(exp_graph);

    experiments::run_random_queries(
        n,
        &mut server,
        &mut rng,
        &mut &mut algo_runs_ctxt,
        rust_road_router::experiments::num_dijkstra_queries(),
    );

    Ok(())
}
