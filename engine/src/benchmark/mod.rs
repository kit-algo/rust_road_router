use time;

pub fn measure<Out, F: FnOnce() -> Out>(name: &str, f: F) -> Out {
    let start = time::now();
    let res = f();
    println!("{}: {}ms", name, (time::now() - start).num_milliseconds());
    res
}
