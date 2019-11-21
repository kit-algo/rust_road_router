use std::env;

use bmw_routing_engine::{io::*, shortest_path::node_order::NodeOrder};

fn main() {
    let mut args = env::args();
    args.next();

    let input = &args.next().expect("No directory input given");
    let output = &args.next().expect("No directory output given");

    let order = NodeOrder::from_ranks(Vec::load_from(input).expect("could not read input"));
    order.order().write_to(output).expect("could not write to output");
}
