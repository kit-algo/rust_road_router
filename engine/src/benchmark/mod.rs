use time;

pub fn measure<Out, F: FnOnce() -> Out>(name: &str, f: F) -> Out {
    let start = time::now();
    println!("starting {}", name);
    let res = f();
    println!("{} done - took: {}ms", name, (time::now() - start).num_milliseconds());
    res
}
