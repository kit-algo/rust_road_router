// Utility to invert a node order that is turn a permutation into a ranking or vice versa.

use std::{env, error::Error};

use rust_road_router::{cli::CliErr, datastr::node_order::NodeOrder, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let input = &args.next().ok_or(CliErr("No input arg given"))?;
    let output = &args.next().ok_or(CliErr("No output arg given"))?;

    let order = NodeOrder::from_ranks(Vec::load_from(input)?);
    order.order().write_to(output)?;

    Ok(())
}
