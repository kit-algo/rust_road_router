use std::{env, error::Error};

use bmw_routing_engine::{cli::CliErr, io::*, shortest_path::node_order::NodeOrder};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let input = &args.next().ok_or(CliErr("No input arg given"))?;
    let output = &args.next().ok_or(CliErr("No output arg given"))?;

    let order = NodeOrder::from_ranks(Vec::load_from(input)?);
    order.order().write_to(output)?;

    Ok(())
}
