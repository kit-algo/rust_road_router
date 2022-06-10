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
    let cch = CCH::fix_order_and_build(&graph, order);

    #[cfg(all(feature = "remove-inf", not(feature = "directed")))]
    let cch = without_reporting(|| cch.remove_always_infinity());

    #[cfg(feature = "directed")]
    let cch = without_reporting(|| cch.to_directed_cch());

    let mut customizations_ctxt = push_collection_context("customizations");

    for _ in 0..1 {
        let _run = customizations_ctxt.push_collection_item();

        #[cfg(not(feature = "directed"))]
        let customized = customize(&cch, &graph);
        #[cfg(feature = "directed")]
        let customized = customize_directed(&cch, &graph);

        #[cfg(all(feature = "perfect-customization", not(feature = "directed")))]
        customize_perfect(customized);
        #[cfg(all(feature = "perfect-customization", feature = "directed"))]
        customize_directed_perfect(customized);
    }

    Ok(())
}
