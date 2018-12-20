use nail;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        match args.get(1).unwrap().as_str() {
            "depoch" => {
                if args.len() > 2 {
                    // iterate over files
                    let (_program, files) = args.split_at(2);
                    nail::process_files(files);
                } else {
                    nail::process_stdin();
                }
            },
            "enhex" => {
                let (_pre, post) = args.split_at(2);
                nail::enhex(post);
            },
            "dehex" => {
                let (_pre, post) = args.split_at(2);
                nail::dehex(post);
            },
            _ => println!("Unknown command: {}", args.get(1).unwrap())
        }
    }
}
