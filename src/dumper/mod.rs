use reqwest::{Client,ClientBuilder};
use reqwest::Error as HttpError;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use futures::future::join_all;
use std::{env,fs,process};
use std::io::{BufReader, BufWriter};
use flate2::Compression;
use flate2::bufread::{GzEncoder};
use std::io::Error as IOError;
use std::io::prelude::*;
use s3::bucket::Bucket as S3Bucket;
use s3::credentials::Credentials as S3Credentials;
use crate::dumper::scroller::Scroller;

mod scroller;

pub struct Dumper {
    client: Client,
    creds: Credentials,
    url: String,
    bucket: Option<S3Bucket>,
}

#[derive(Clone)]
pub struct Credentials {
    username: String,
    password: String,
}

impl Dumper {
    //===================== DUMPER ==========================
    // This object implements a REST client for interacting //
    // with Dumper to incrementally dump indices to disk.   //
    // An almost exact clone of ElasticDump but rustic.     //
    // ======================================================
    pub fn new(url: &str, username: &str, password: &str) -> Result<Dumper, HttpError> {
        // Create new client with API base URL - default 10s timeout
        let ten_seconds = Duration::new(10, 0);
        let invalid_certs = !(env::var("TLS_NO_VERIFY").unwrap_or("".to_string()) == "");
        let client = ClientBuilder::new().timeout(ten_seconds)
            .danger_accept_invalid_certs(invalid_certs)
            .build()?;

        // Setup creds
        let creds = Credentials{username: username.to_string(), 
            password: password.to_string()};

        Ok(Dumper{client: client, url: url.to_string(), creds: creds, bucket: None})
    }

    pub fn new_with_bucket(url: &str, username: &str, password: &str, bucket_name: &str, access_key: &str, secret_key: &str, region: &str) -> Result<Dumper, HttpError> {
        // Create new client with API base URL - default 10s timeout
        let ten_seconds = Duration::new(10, 0);

        // TODO: only accept invalid certs if told to do so...
        // i.e. read from environment variable or force CA otherwise
        let client = ClientBuilder::new().timeout(ten_seconds)
            .danger_accept_invalid_certs(true)
            .build()?;

        // Setup creds
        let creds = Credentials{username: username.to_string(), 
            password: password.to_string()};

        // Setup bucket
        let s3creds: S3Credentials = S3Credentials::new(Some(access_key.to_string()),Some(secret_key.to_string()), None, None);

        let bucket: S3Bucket = S3Bucket::new(bucket_name, region.parse().unwrap(), s3creds).expect("Unable to create S3 bucket option");
        
        // Test bucket configuration
        let content = "LZF".as_bytes();
        let (_, code) = bucket.put_object("/tana-put-test-object", content, "text/plain").unwrap();
        if code != 200 {
            println!("PUT OBJECT: {} - Incorrect details for Bucket, ensure access keys, bucket name and region are correct.", code);
            process::exit(1);
        } else {
            println!("Successfully verified PUT access to '{}' bucket.", bucket_name);
        }

        Ok(Dumper{client: client, url: url.to_string(), creds: creds, bucket: Some(bucket)})
    }


    pub async fn get_indices(&mut self) -> Result<Vec<String>, HttpError> {
        // Use _cat/indices to get the names of all the indices
        let url = format!("{}/_cat/indices?h=index", &self.url);

        let res = self.client.get(&url).basic_auth(&self.creds.username,
            Some(&self.creds.password)).send().await?;

        let text = res.text().await?;

        // Split and collect them - POSSIBLY RETURN ITERATOR INSTEAD?
        let indices = text.split("\n").map(|index|{index.to_string()})
            .collect::<Vec<String>>();

        Ok(indices)
    }

    pub async fn dump_index_to_disk(self, index: String, slices: i64) -> Result<(), HttpError> {
        // Create a lot of work for ourselves
        //   They each need their own copy of the url, credentials and index
        let mut work = vec![];
        for i in 0..slices {
            println!("Creating scroller {}", i);
            let scroller = Scroller::new(self.url.clone(), index.clone(), i, slices)?;
            work.push(scroller.scroll(self.creds.clone()));
        }

        // Wait for that work to finish
        join_all(work).await;

        // Consolidate files
        Dumper::consolidate_files(&*index).await.expect("Unable to consolidate files.");

        Ok(())
    }


    pub async fn crunch_index_to_disk(self, index: String, slices: i64) -> Result<(), HttpError> {

        // Create a lot of work for ourselves
        //   They each need their own copy of the url, credentials and index
        let mut work = vec![];
        for i in 0..slices {
            println!("Creating scroller {}", i);
            let scroller = Scroller::new(self.url.clone(), index.clone(), i, slices+1)?;
            work.push(scroller.scroll(self.creds.clone()));
        }

        // Wait for that work to finish
        join_all(work).await;

        // Consolidate files
        Dumper::consolidate_files(&*index).await.expect("Unable to consolidate files.");

        // Compress the result
        Dumper::compress_final(index).await.expect("Unable to compress final file.");

        Ok(())
    }

    pub async fn dump_index_to_s3(self, index: String, slices: i64) -> Result<(), HttpError> {

        // Create a lot of work for ourselves
        //   They each need their own copy of the url, credentials and index
        let mut work = vec![];
        for i in 0..slices {
            println!("Creating scroller {}", i);
            let scroller = Scroller::new(self.url.clone(), index.clone(), i, slices+1)?;
            work.push(scroller.scroll(self.creds.clone()));
        }

        // Wait for that work to finish
        join_all(work).await;

        // Consolidate files
        Dumper::consolidate_files(&*index).await.expect("Unable to consolidate files.");

        // Send final file to S3
        let file: Vec<u8> = fs::read(format!("{}.json", index)).expect("Unable to open target file.");
        let metadata = fs::metadata(format!("{}.json", index)).expect("Unable to read metadata of target file.");
        if self.bucket.is_some() && metadata.len() != 0 {
            println!("PUT {} to S3.", index);
            let (_, code) = self.bucket.unwrap().put_object(&format!("{}.json", index), &file, "data/binary").unwrap();
            if code != 200 {
                println!("Error putting file to S3: {}", code);
                process::exit(1);
            } else {
                println!("Success. Removing {} files.", index);
                fs::remove_file(format!("{}.json", index));
            }
        }

        Ok(())
    }

    pub async fn crunch_index_to_s3(self, index: String, slices: i64) -> Result<(), HttpError> {

        // Create a lot of work for ourselves
        //   They each need their own copy of the url, credentials and index
        let mut work = vec![];
        for i in 0..slices {
            println!("Creating scroller {}", i);
            let scroller = Scroller::new(self.url.clone(), index.clone(), i, slices+1)?;
            work.push(scroller.scroll(self.creds.clone()));
        }

        // Wait for that work to finish
        join_all(work).await;

        // Consolidate files
        Dumper::consolidate_files(&*index).await.expect("Unable to consolidate files.");

        // Compress the result
        Dumper::compress_final(index.clone()).await.expect("Unable to compress final file.");

        // Send final file to S3
        let file: Vec<u8> = fs::read(format!("{}.gz", index)).expect("Unable to open target file.");
        let metadata = fs::metadata(format!("{}.gz", index)).expect("Unable to read metadata of target file.");
        if self.bucket.is_some() && metadata.len() != 0 {
            println!("PUT {} to S3.", index);
            let (_, code) = self.bucket.unwrap().put_object(&format!("{}.gz", index), &file, "data/binary").unwrap();
            if code != 200 {
                println!("Error putting file to S3: {}", code);
                process::exit(1);
            } else {
                println!("Success. Removing {} files.", index);
                fs::remove_file(format!("{}.gz", index));
                fs::remove_file(format!("{}.json", index));
            }
        }

        Ok(())
    }


    async fn consolidate_files(index: &str) -> Result<(), IOError> {
        // Finds all individual files and cats them together in non-deterministic order
        let current_dir = env::current_dir()?;
        let destination_file = format!("{}.json", index);
        let new_file = fs::OpenOptions::new().create(true).append(true)
            .open(&destination_file)
            .expect("Unable to open consolidation file.");
     
        let cat_writer = BufWriter::new(new_file);
        let writer_mutex = Arc::new(Mutex::new(cat_writer));
     
        // We know all temp files are written in current dir
        let mut work = vec![];
        for entry in fs::read_dir(current_dir)? {
            let entry = entry?;
            let path = entry.path().to_str().unwrap_or("").to_string();
            if path.contains(index) && !path.contains(&destination_file) {
                println!("Consolidating {:?}", entry);
                work.push(Dumper::buffered_write(path, writer_mutex.clone()));
            }
        }
     
         // Wait for workers to return
         join_all(work).await;
     
         Ok(())
    }

    async fn compress_final(index: String) -> Result<(), IOError> {
        let in_file = fs::File::open(format!("{}.json", index)).expect("Unable to open uncompressed JSON file.");
        let mut out_file = fs::OpenOptions::new().create(true).append(true)
            .open(format!("{}.gz", index))
            .expect("Unable to open target compressed file.");

        let reader = BufReader::new(in_file);
        let z_stream = GzEncoder::new(reader, Compression::best()).bytes();
        println!("Compressing final result...");
        for byte in z_stream.map(|b| b.unwrap()) {
            out_file.write(&vec![byte])?;
        }

        Ok(())
    }

    async fn buffered_write(path: String, writer_mutex: Arc<Mutex<BufWriter<fs::File>>>) -> Result<(), IOError> {
        // Takes a writer object and writes entries from a BufReader
        let in_file = fs::File::open(&path).expect("Unable to open file.");
        let reader = BufReader::new(in_file);
        for line in reader.lines() {
            // Obtain the lock and write
            let mut new_line = line?;
            new_line.push('\n');
            let mut writer = writer_mutex.lock().expect("Cannot obtain lock for writer.");
            writer.write(&new_line.as_bytes())?;

        }

        fs::remove_file(path)?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn correct_formatting() {
	let created = Dumper::new_with_bucket("https://localhost:9200",
	    "elastic",
	    "test-account",
	    "dummy-bucket",
	    "FAKE",
	    "NOSECRETS",
	    "eu-west-1");
	// Get the indices
	let elastic_dumper = created.unwrap();
	// Consume and dump to disk without compression 
        let slices: i64 = 3;
	let dumped = elastic_dumper.dump_index_to_disk("logs".to_string(), slices).await;	
	let file = std::fs::File::open("logs.json").unwrap();
        let reader = std::io::BufReader::new(file);
        verify_lines(reader);
    }

    fn verify_lines(reader: BufReader<fs::File>) {
        // Verify every other line contains "index" and every other line contains "@timestamp"
        let mut i: i32 = 0;
	for line in reader.lines() {
            let text = line.unwrap();
            if i%2 == 0 {
                if !text.contains("index") {
                    println!("Failing line {}", i);
                    assert!(text.contains("index"));
                }
            } else {
                if !text.contains("@timestamp") {
                    println!("Failing line {}", i);
                    assert!(text.contains("@timestamp"));
                }
            }
            i += 1; 
	}
    }
}
