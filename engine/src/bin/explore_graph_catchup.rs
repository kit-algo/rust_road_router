// Utility to gather some stats over CATCHUp customization output.

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    cli::CliErr,
    datastr::graph::{floating_time_dependent::*, *},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    graph.report_relative_delays();

    let mut first_out = Vec::<u32>::load_from(path.join("customized/incoming_first_source"))?;
    let mut other_first_out = Vec::<u32>::load_from(path.join("customized/outgoing_first_source"))?;
    let first_last = first_out.pop().unwrap();
    for idx in &mut other_first_out {
        *idx += first_last;
    }
    first_out.append(&mut other_first_out);

    report!("cch_arcs", first_out.len() - 1);
    report!("mean_num_expansions", f64::from(*first_out.last().unwrap()) / (first_out.len() - 1) as f64);
    report!(
        "num_arcs_with_one_expansion",
        first_out.windows(2).filter(|w| w[1] - w[0] == 1).count() as f64 / (first_out.len() - 1) as f64
    );
    report!("max_num_expansions", first_out.windows(2).map(|w| w[1] - w[0]).max().unwrap());

    Ok(())
}
