use crate::dumper::Credentials;
use reqwest::Error as HttpError;
use reqwest::{Client, ClientBuilder};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fs;
use std::io::prelude::*;
use std::time::Duration;

pub struct Scroller {
    index: String,
    client: Client,
    url: String,
    slice_id: i64,
    max_slices: i64,
}
#[derive(Serialize, Debug)]
struct ActionMeta {
    index: Index,
}

#[derive(Serialize, Debug)]
struct Index {
    _index: String,
    _id: String,
}

impl Scroller {
    pub fn new(
        url: String,
        index: String,
        slice_id: i64,
        max_slices: i64,
    ) -> Result<Scroller, HttpError> {
        // Create new client with API base URL - default 10s timeout
        let ten_seconds = Duration::new(10, 0);
        let client = ClientBuilder::new()
            .timeout(ten_seconds)
            .danger_accept_invalid_certs(true)
            .build()?;

        Ok(Scroller {
            index: index,
            slice_id: slice_id,
            max_slices: max_slices,
            client: client,
            url: url,
        })
    }

    pub async fn scroll(self, creds: Credentials) -> Result<i64, HttpError> {
        // We only want to consume 1000 documents at a time
        let url = format!("{}/{}/_search?scroll=1m&size=1000", &self.url, &self.index);
        let payload = format!(
            "{{
        \"slice\": {{
                \"id\": {}, 
                \"max\": {} 
        }},
        \"sort\": [\"_doc\"]
        }}",
            self.slice_id, self.max_slices
        );

        let json_str = self
            .client
            .get(&url)
            .body(payload)
            .header("Content-Type", "application/json")
            .basic_auth(&creds.username, Some(&creds.password))
            .send()
            .await?
            .text()
            .await?;

        // Parse the JSON response and keep it here in memory
        let (mut total_hits_json, hit_count, scroll_id) = Scroller::parse_json(json_str);

        // Scroll to end
        total_hits_json = format!(
            "{}{}",
            total_hits_json,
            self.scroll_to_end(&creds, scroll_id.clone()).await?
        );

        // Dump to disk
        println!(
            "Writing {:?} documents.",
            total_hits_json.matches("\n").count() / 2
        );
        Scroller::write_to_disk(
            format!("{}-{}.json", &self.index, &self.slice_id),
            total_hits_json,
        );

        // Clear the scroll_id so Elasticsearch doesn't run out of file handles!
        self.clear(creds, scroll_id).await?;

        Ok(hit_count)
    }

    async fn scroll_to_end(
        &self,
        creds: &Credentials,
        mut scroll_id: String,
    ) -> Result<String, HttpError> {
        // Make the request for this crumb
        let url = format!("{}/_search/scroll", &self.url);
        let mut hit_count: i64 = -1;
        let mut total_hits_json = "".to_string();
        let mut hits_json: String;
        while hit_count != 0 {
            let payload = format!(
                "{{
                  \"scroll\": \"1m\",
                  \"scroll_id\": {}
            }}",
                scroll_id
            );

            let json_str = self
                .client
                .post(&url)
                .body(payload)
                .header("Content-Type", "application/json")
                .basic_auth(&creds.username, Some(&creds.password))
                .send()
                .await?
                .text()
                .await?;

            // Parse the JSON and keep it in memory
            let parse_tuple = Scroller::parse_json(json_str);
            hits_json = parse_tuple.0;
            hit_count = parse_tuple.1;
            scroll_id = parse_tuple.2;
            if hits_json != "" {
                println!("Writing 1000 entries [TOTAL {}]", hit_count);
                total_hits_json = format!("{}{}", total_hits_json, hits_json);
            } else {
                hit_count = 0
            }
        }
        Ok(total_hits_json)
    }

    fn parse_json(json_str: String) -> (String, i64, String) {
        let v: JsonValue = serde_json::from_str(&json_str).expect("Unable to parse JSON.");

        // We safely presume hits is an Array.
        let hits = &v["hits"]["hits"];
        let mut hits_json = "".to_string();
        let mut hits_vec: &Vec<JsonValue> = &vec![];
        hits_vec = hits.as_array().unwrap_or(hits_vec);

        let offset = &v["hits"]["hits"][0]["sort"][0];
        let hit_count = v["hits"]["total"]["value"].as_i64().unwrap_or(0);

        for hit in hits_vec.iter() {
            let index = Index {
                _index: hit["_index"].as_str().unwrap_or("").to_string(),
                _id: hit["_id"].as_str().unwrap_or("").to_string(),
            };
            let action_meta = ActionMeta { index: index };
            let source = &hit["_source"];
            let action_meta_parsed = serde_json::to_string(&action_meta);
            let source_parsed = serde_json::to_string(&source);
            if action_meta_parsed.is_ok() && source_parsed.is_ok() {
                // Append action meta for the new document
                hits_json = format!("{}{}", hits_json, action_meta_parsed.unwrap());
                // Append the index data for the new document
                hits_json = format!("{}\n{}\n", hits_json, source_parsed.unwrap());
            } else {
                println!(
                    "Failed to parse action meta or source: {:?} {:?}",
                    action_meta_parsed.err(),
                    source_parsed.err()
                );
            }
        }
        (hits_json, hit_count, v["_scroll_id"].to_string())
    }

    fn write_to_disk(filename: String, hits_json: String) {
        // Get the values we need from the JSON response
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename)
            .expect("Unable to open consolidation file.");

        file.write(hits_json.as_bytes())
            .expect("Unable to write file");
    }

    async fn clear(self, creds: Credentials, scroll_id: String) -> Result<(), HttpError> {
        let url = format!("{}/_search/scroll", &self.url);
        let payload = format!(
            "{{
        \"scroll_id\": {}
        }}",
            scroll_id
        );

        let _json_str = self
            .client
            .delete(&url)
            .body(payload)
            .header("Content-Type", "application/json")
            .basic_auth(&creds.username, Some(&creds.password))
            .send()
            .await?
            .text()
            .await?;

        Ok(())
    }
}
