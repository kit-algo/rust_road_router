use rust_road_router::{cli::CliErr, io::*};
use std::{env, error::Error, fmt::Display};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match &(args.next(), args.next(), args.next()) {
        (Some(data_type), Some(input1), Some(input2)) => {
            match data_type.as_ref() {
                "i8" | "int8" => compare_values::<i8>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "u8" | "uint8" => compare_values::<u8>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "i16" | "int16" => compare_values::<i16>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "u16" | "uint16" => compare_values::<u16>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "i32" | "int32" => compare_values::<i32>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "u32" | "uint32" => compare_values::<u32>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "i64" | "int64" => compare_values::<i64>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "u64" | "uint64" => compare_values::<u64>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "f32" | "float32" => compare_values::<f32>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                "f64" | "float64" => compare_values::<f64>(&Vec::load_from(input1)?, &Vec::load_from(input2)?),
                _ => {
                    print_usage();
                    return Err(Box::new(CliErr("Invalid data type")));
                }
            };
            Ok(())
        }
        _ => {
            print_usage();
            Err(Box::new(CliErr("Invalid arguments")))
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage: decode_vector data_type vector1_file vector2_file

Compares two vectors of elements in binary format. data_type can be one of
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

fn compare_values<T>(values1: &[T], values2: &[T])
where
    T: Display,
    T: PartialOrd,
{
    if values1.len() != values2.len() {
        println!("0");
        eprintln!(
            "Can only compare vectors of equal size. The first vector has {} elements. The second vector has {} elements.",
            values1.len(),
            values2.len()
        );
        return;
    }

    let mut v1_smaller_count = 0;
    let mut v2_smaller_count = 0;
    let mut first_diff = None;

    for (i, (v1, v2)) in values1.iter().zip(values2.iter()).enumerate() {
        if v1 < v2 {
            v1_smaller_count += 1;
        }
        if v2 < v1 {
            v2_smaller_count += 1;
        }

        if first_diff.is_none() && v1 != v2 {
            first_diff = Some(i)
        }
    }

    match first_diff {
        Some(index) => {
            eprintln!("The vectors differ.");
            eprintln!("{} elements are smaller in the first vector.", v1_smaller_count);
            eprintln!("{} elements are smaller in the second vector.", v2_smaller_count);
            eprintln!("{} elements are the same.", values1.len() - v1_smaller_count - v2_smaller_count);
            println!("{}", values1.len() - v1_smaller_count - v2_smaller_count);
            eprintln!("{} elements are different.", v1_smaller_count + v2_smaller_count);
            eprintln!("The vectors have {} elements.", values1.len());
            eprintln!("The first element that differs is at index {}.", index);
        }
        None => {
            println!("{}", values1.len());
            eprintln!("The vectors are the same and have {} elements.", values1.len());
        }
    }
}
