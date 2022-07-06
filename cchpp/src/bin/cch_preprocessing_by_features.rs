use std::{env, error::Error, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_customization_by_features");
    report!("num_threads", rayon::current_num_threads());
    report!("remove_always_infinity", cfg!(feature = "remove-inf"));
    report!("directed_hierarchies", cfg!(feature = "directed"));
    report!("perfect_customization", cfg!(feature = "perfect-customization"));

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join(args.next().unwrap_or("cch_perm".to_string())))?);

    let mut preprocessings_ctxt = push_collection_context("preprocessings");

    for _ in 0..100 {
        let _run = preprocessings_ctxt.push_collection_item();

        report_time_with_key("Full Preprocessing", "total_preprocessing_ms", || {
            let _cch = CCH::fix_order_and_build(&graph, order.clone());

            #[cfg(all(feature = "remove-inf", not(feature = "directed")))]
            let _cch = without_reporting(|| _cch.remove_always_infinity());

            #[cfg(feature = "directed")]
            let _cch = without_reporting(|| _cch.to_directed_cch());
        });
    }

    Ok(())
}
