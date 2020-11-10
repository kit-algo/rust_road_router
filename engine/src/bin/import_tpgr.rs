// Graph processing utility to scale the weights of a tpgr graph (because KaTCH uses tenths of seconds as the base unit).

use rust_road_router::{
    cli::CliErr,
    datastr::graph::{time_dependent::Timestamp, *},
    io::*,
};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let input = &args.next().ok_or(CliErr("No input tpgr given"))?;
    let input = Path::new(input);

    let output = &args.next().ok_or(CliErr("No output dir"))?;
    let out_dir = Path::new(output);

    let input = File::open(input)?;
    let mut lines = BufReader::new(&input).lines();

    let header = lines.next().unwrap()?;
    let mut header = header.split_whitespace().map(|number| number.parse::<usize>().unwrap());
    let n = header.next().unwrap();
    let m = header.next().unwrap();
    let p = header.next().unwrap();
    let period = header.next().unwrap();

    assert!(n <= std::u32::MAX as usize);
    assert!(m <= std::u32::MAX as usize);
    assert!(p <= std::u32::MAX as usize);

    assert_eq!(period, 86400);

    let mut first_out: Vec<EdgeId> = Vec::with_capacity(n + 1);
    let mut head: Vec<NodeId> = Vec::with_capacity(m);
    let mut first_ipp_of_arc: Vec<u32> = Vec::with_capacity(m + 1);
    let mut ipp_departure_time: Vec<Timestamp> = Vec::with_capacity(p);
    let mut ipp_travel_time: Vec<Weight> = Vec::with_capacity(p);

    first_out.push(0);
    first_ipp_of_arc.push(0);

    for line in lines {
        let line = line.unwrap();
        let mut words = line.split_whitespace();
        let tail = words.next().unwrap().parse::<usize>().unwrap();
        let edge_head = words.next().unwrap().parse::<NodeId>().unwrap();
        let ipp_count = words.next().unwrap().parse::<usize>().unwrap();

        assert!(tail >= first_out.len() - 1);
        while tail > first_out.len() - 1 {
            first_out.push(head.len() as EdgeId);
        }

        head.push(edge_head);

        for _ in 0..ipp_count {
            let at = (words.next().unwrap().parse::<f64>().unwrap() * 1000.0) as Timestamp;
            ipp_departure_time.push(at);
            let val = (words.next().unwrap().parse::<f64>().unwrap() * 1000.0) as Weight;
            ipp_travel_time.push(val);
        }
        debug_assert_eq!(words.next(), None);

        first_ipp_of_arc.push(ipp_departure_time.len() as u32);
    }

    assert!(n >= first_out.len() - 1);
    while n >= first_out.len() {
        first_out.push(head.len() as EdgeId);
    }

    first_out.write_to(&out_dir.join("first_out"))?;
    head.write_to(&out_dir.join("head"))?;
    first_ipp_of_arc.write_to(&out_dir.join("first_ipp_of_arc"))?;
    ipp_departure_time.write_to(&out_dir.join("ipp_departure_time"))?;
    ipp_travel_time.write_to(&out_dir.join("ipp_travel_time"))?;

    Ok(())
}
