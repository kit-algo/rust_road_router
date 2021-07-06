use rust_road_router::{cli::CliErr, io::*};
use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match (args.next(), args.next()) {
        (Some(data_type), Some(ref input)) => match data_type.as_ref() {
            "i8" | "int8" => print_values(Vec::<i8>::load_from(input)?),
            "u8" | "uint8" => print_values(Vec::<u8>::load_from(input)?),
            "i16" | "int16" => print_values(Vec::<i16>::load_from(input)?),
            "u16" | "uint16" => print_values(Vec::<u16>::load_from(input)?),
            "i32" | "int32" => print_values(Vec::<i32>::load_from(input)?),
            "u32" | "uint32" => print_values(Vec::<u32>::load_from(input)?),
            "i64" | "int64" => print_values(Vec::<i64>::load_from(input)?),
            "u64" | "uint64" => print_values(Vec::<u64>::load_from(input)?),
            "f32" | "float32" => print_values(Vec::<f32>::load_from(input)?),
            "f64" | "float64" => print_values(Vec::<f64>::load_from(input)?),
            _ => {
                print_usage();
                return Err(Box::new(CliErr("Invalid data type")));
            }
        },
        _ => {
            print_usage();
            return Err(Box::new(CliErr("Invalid arguments")));
        }
    };
    Ok(())
}

fn print_usage() {
    eprintln!(
        "Usage: decode_vector data_type input_vector_file

Reads binary data from input_vector_file and writes the data to the standard output. data_type can be one of
* i8
* u8
* i16
* u16
* i32
* u32
* i64
* u64
* f32
* f64

"
    );
}

use std::fmt::Display;

fn print_values<T>(values: Vec<T>)
where
    T: Display,
{
    for v in values {
        println!("{}", v);
    }
}
