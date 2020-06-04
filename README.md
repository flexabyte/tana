# TANA
This tool is named after the largest landfill compactor. Pull and crunch terabytes of Elasticsearch data in blazing fast speeds with concurrency by default.
Originally designed to reduce the cost of Cloud backup pricing. When backing up in AWS directly from Elasticsearch we take the hit of uncompressed backups that contain duplicated data due to sharding. We want to dump the raw JSON to file, optionally compress it beforehand and send it to S3, or another Elasticsearch instance.

Currently under active development still, but a scheduled stable release is planned. 

# USAGE

Currently only dumps JSON to file for specified indices, and supports dumping all indices to file. The file output can be used with `curl` later to `POST` it straight back to another index or Elasticsearch cluster entirely.

Use it in your projects:

```
    // New Dumper object
    let created = Dumper::new("https://localhost:9200",
        "elastic",
        "changeme");

    if !created.is_ok() {
        println!("Unable to create Elasticsearch object.");
        process::exit(1);
    }

    // Get the indices
    let mut elastic_dumper = created.unwrap();
    let indices = elastic_dumper.get_indices().await;

    // Dump an index to disk
    let mut dumped = elastic_dumper.dump_index("winlogbeat-2020.04.20".to_string()).await;

    if !dumped.is_ok() {
        println!("Unable to dump elasticsearch index.");
        process::exit(1);
    }

    // Dump all indices to disk
    for index in indices {
        dumped = elastic_dumper.dump_index(index).await;

        if !dumped.is_ok() {
            println!("Unable to dump elasticsearch index.");
        }
    }

```

Or just use the command line tool:

```
USAGE:
    tana [FLAGS] [OPTIONS]

FLAGS:
    -g, --gzip       If supplied, compress output using gzip
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --bucket <bucket>              The destination S3 bucket name.
    -c, --concurrency <concurrency>    The number of scrollers to create. Each scroller can make its own concurrent
                                       requests.
    -d, --dest <destination>           The destination type: [disk|s3]
    -i, --index <index>                The name of the index you wish to dump
    -r, --region <region>              The AWS region where the S3 bucket resides. E.g. [eu-west-2].

```
