use csv::ReaderBuilder;
use std::{env, error::Error, fs::File, path::Path};

use rust_road_router::{cli::CliErr, datastr::graph::*, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;

    let file = File::open(args.next().unwrap()).unwrap();
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .quoting(false)
        .double_quote(false)
        .escape(None)
        .from_reader(file);

    let mut live = Vec::<(EdgeId, Weight, Weight)>::new();

    for line in reader.records() {
        let record = line?;
        let from = record[0].parse()?;
        let to = record[1].parse()?;
        let speed: u32 = record[2].parse()?;
        let distance: u32 = record[3].parse()?;
        let duration: u32 = record[4].parse()?;

        if let Some(EdgeIdT(edge_id)) = graph.edge_indices(from, to).next() {
            let new_tt = if speed == 0 { INFINITY } else { 100 * 36 * distance / speed };
            live.push((edge_id, new_tt, duration.saturating_mul(1000)));
        }
    }

    live.write_to(&path.join(args.next().as_deref().unwrap_or("live_data")))?;

    Ok(())
}
