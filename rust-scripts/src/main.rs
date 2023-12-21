extern crate flate2;

use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp::max;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

#[derive(Serialize, Deserialize)]
struct TweetRecord {
    ts: u32,
    id: u64,
    tweet: Map<String, Value>,
}

fn main() {
    let mut tweet_map: HashMap<u64, u32> = HashMap::new();

    let file = File::open("tweets/2023-03-14.gz").unwrap();
    let reader = BufReader::new(MultiGzDecoder::new(file));

    let mut counter = 0;
    for line in reader.lines() {
        counter += 1;

        let record: Result<TweetRecord, serde_json::Error> = serde_json::from_str(&line.unwrap());
        let Ok(tweet_record) = record else { continue };

        let mut sub_ids = vec![tweet_record.id];

        if tweet_record.tweet.contains_key("retweeted_status")
            && tweet_record.tweet["retweeted_status"]["id"] != sub_ids[0]
        {
            sub_ids.push(
                tweet_record.tweet["retweeted_status"]["id"]
                    .as_u64()
                    .unwrap(),
            );
            println!("{}, {}", sub_ids[0], sub_ids[1]);
        } else if tweet_record.tweet.contains_key("quoted_status")
            && tweet_record.tweet["quoted_status"]["id"] != sub_ids[0]
        {
            sub_ids.push(tweet_record.tweet["quoted_status"]["id"].as_u64().unwrap());
            println!("{}, {}", sub_ids[0], sub_ids[1]);
        }

        for sub_id in sub_ids {
            tweet_map
                .entry(sub_id)
                .and_modify(|e| *e = max(*e, tweet_record.ts))
                .or_insert(tweet_record.ts);
        }

        if counter > 30 {
            break;
        }
    }
    println!("{:?}", tweet_map)
}
