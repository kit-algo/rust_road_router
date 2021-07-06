use rust_road_router::{cli::CliErr, datastr::node_order::*, io::*};
use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let input = &args.next().ok_or(CliErr("No input arg given"))?;
    let data_type = &args.next().ok_or(CliErr("No type arg given"))?;
    let order = &args.next().ok_or(CliErr("No permutation arg given"))?;
    let output = &args.next().ok_or(CliErr("No output arg given"))?;

    let order = NodeOrder::from_ranks(Vec::<u32>::load_from(order)?);

    match data_type.as_ref() {
        "i8" => permute(Vec::<i8>::load_from(input)?, order).write_to(output)?,
        "u8" => permute(Vec::<u8>::load_from(input)?, order).write_to(output)?,
        "i16" => permute(Vec::<i16>::load_from(input)?, order).write_to(output)?,
        "u16" => permute(Vec::<u16>::load_from(input)?, order).write_to(output)?,
        "i32" => permute(Vec::<i32>::load_from(input)?, order).write_to(output)?,
        "u32" => permute(Vec::<u32>::load_from(input)?, order).write_to(output)?,
        "i64" => permute(Vec::<i64>::load_from(input)?, order).write_to(output)?,
        "u64" => permute(Vec::<u64>::load_from(input)?, order).write_to(output)?,
        "f32" => permute(Vec::<f32>::load_from(input)?, order).write_to(output)?,
        "f64" => permute(Vec::<f64>::load_from(input)?, order).write_to(output)?,
        "int8" => permute(Vec::<i8>::load_from(input)?, order).write_to(output)?,
        "uint8" => permute(Vec::<u8>::load_from(input)?, order).write_to(output)?,
        "int16" => permute(Vec::<i16>::load_from(input)?, order).write_to(output)?,
        "uint16" => permute(Vec::<u16>::load_from(input)?, order).write_to(output)?,
        "int32" => permute(Vec::<i32>::load_from(input)?, order).write_to(output)?,
        "uint32" => permute(Vec::<u32>::load_from(input)?, order).write_to(output)?,
        "int64" => permute(Vec::<i64>::load_from(input)?, order).write_to(output)?,
        "uint64" => permute(Vec::<u64>::load_from(input)?, order).write_to(output)?,
        "float32" => permute(Vec::<f32>::load_from(input)?, order).write_to(output)?,
        "float64" => permute(Vec::<f64>::load_from(input)?, order).write_to(output)?,
        _ => return Err(Box::new(CliErr("Invalid data type"))),
    };

    Ok(())
}

fn permute<T: Copy>(data: Vec<T>, order: NodeOrder) -> Vec<T> {
    let mut permutated = data.clone();
    for (rank, &node) in order.order().iter().enumerate() {
        permutated[rank] = data[node as usize];
    }
    permutated
}
