use crate::dumper::Dumper;
use std::process;
use std::env;

mod dumper;
mod app;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main()  {

    // Command line arguments
    let matches = app::parse_args();

    // Our Dumper
    let created: Result<Dumper,reqwest::Error>;

    // Check we have environment variables first
    let elastic_host = env::var("ES_HOST").expect("No Elasticsearch host given. Please set ES_HOST environment variable.");
    let elastic_user = env::var("ES_USER").unwrap_or("".to_string());
    let elastic_pass = env::var("ES_PASS").unwrap_or("".to_string());

    // Now check destination and source
    let destination = matches.value_of("destination").expect("Empty destination supplied. Use the '--dest' argument");
    let index = matches.value_of("index").expect("No index supplied. Use the '--index' argument");
    let slices: i64 = matches.value_of("concurrency").unwrap_or("3").parse().unwrap();
    if destination == "s3" {
        // If s3: check more environment variables
        let s3_access_key = env::var("S3_ACCESS_KEY").expect("No AWS access key ID provided. Set the S3_ACCESS_KEY environment variable.");
        let s3_secret_key = env::var("S3_SECRET_KEY").expect("No AWS secret access key provided. Set the S3_SECRET_KEY environment variable.");
        // Bucket name
        let s3_bucket_name = matches.value_of("bucket").expect("No S3 bucket name provided. Use the '--bucket' argument");
        // Region
        let s3_region = matches.value_of("region").expect("No AWS region supplied. Use the '--region' argument");

        // Create an S3 dumper
        created = Dumper::new_with_bucket(&elastic_host, &elastic_user, &elastic_pass,
            &s3_bucket_name, &s3_access_key, &s3_secret_key, &s3_region);

    } else if destination == "disk" {
        // Create a Disk dumper
        created = Dumper::new(&elastic_host, &elastic_user, &elastic_pass)
    } else {
        println!("Destination: {} is not supported. Must be [s3|disk].", destination);
        process::exit(1);
    }

    // Sanity check before unwrapping
    if !created.is_ok() {
        println!("Unable to create Elasticsearch object.");
        process::exit(1);
    }

    let elastic_dumper = created.unwrap();

    // Run the CMD 
    let compression = matches.is_present("compression");
    let dumped: Result<(), reqwest::Error>;
    if compression && destination == "s3" {
        println!("Writing compressed to S3.");
        dumped = elastic_dumper.crunch_index_to_s3(index.to_string(), slices).await;
        if !dumped.is_ok() {
            println!("Unable to dump elasticsearch index.");
            process::exit(1);
        } else {
            println!("Successfully Completed.");
        }
    } else if compression && destination == "disk" {
        println!("Writing compressed to disk.");
        dumped = elastic_dumper.crunch_index_to_disk(index.to_string(), slices).await;
        if !dumped.is_ok() {
            println!("Unable to dump elasticsearch index.");
            process::exit(1);
        } else {
            println!("Successfully Completed.");
        }
    } else if !compression && destination == "s3" {
        println!("Writing uncompressed to S3.");
        dumped = elastic_dumper.dump_index_to_s3(index.to_string(), slices).await;
        if !dumped.is_ok() {
            println!("Unable to dump elasticsearch index.");
            process::exit(1);
        } else {
            println!("Successfully Completed.");
        }
    } else if !compression && destination == "disk" {
        println!("Writing uncompressed to disk.");
        dumped = elastic_dumper.dump_index_to_disk(index.to_string(), slices).await;
        if !dumped.is_ok() {
            println!("Unable to dump elasticsearch index.");
            process::exit(1);
        } else {
            println!("Successfully Completed.");
        }
    } 

}

