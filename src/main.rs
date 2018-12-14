use nail;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        // iterate over files
        if let Some((_program, files)) = args.split_first() {
            nail::process_files(files);
        }
    }
    println!("Hello, world!");
}
