use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use clap::Parser;
use kube::CustomResourceExt;
use search_controller::SearchIndex;

/// Generate SearchIndex CRD YAML
#[derive(Parser)]
#[command(
    name = "crdgen",
    about = "Generate SearchIndex CustomResourceDefinition YAML"
)]
struct Args {
    /// Output file (stdout if not specified)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let yaml = serde_yaml::to_string(&SearchIndex::crd()).unwrap();

    match args.output {
        Some(path) => {
            let mut file = File::create(&path).expect("Failed to create output file");
            file.write_all(yaml.as_bytes())
                .expect("Failed to write CRD");
            eprintln!("Wrote CRD to {}", path.display());
        }
        None => {
            print!("{}", yaml);
        }
    }
}
