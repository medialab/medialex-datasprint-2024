extern crate flate2;

use csv::Writer;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp::max;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Serialize, Deserialize)]
struct TweetRecord {
    ts: u32,
    id: u64,
    tweet: Map<String, Value>,
}

fn main() {
    let mut tweet_map: HashMap<u64, u32> = HashMap::new();
    let inpath = "tweets/2023-03-14.gz";

    let file = File::open(inpath).unwrap();
    let reader = BufReader::new(MultiGzDecoder::new(file));

    let bar = ProgressBar::new(9000000);
    bar.set_style(
        ProgressStyle::with_template(
            "Read file: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} [{eta_precise}]",
        )
        .unwrap(),
    );
    for line in reader.lines() {
        bar.inc(1);

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
        } else if tweet_record.tweet.contains_key("quoted_status")
            && tweet_record.tweet["quoted_status"]["id"] != sub_ids[0]
        {
            sub_ids.push(tweet_record.tweet["quoted_status"]["id"].as_u64().unwrap());
        }

        for sub_id in sub_ids {
            tweet_map
                .entry(sub_id)
                .and_modify(|e| *e = max(*e, tweet_record.ts))
                .or_insert(tweet_record.ts);
        }
    }
    // Leave the current progress
    bar.abandon();

    let outpath = inpath.replace(".gz", "_unique_ids.csv");
    let mut writer = Writer::from_path(outpath).unwrap();
    let bar = ProgressBar::new(tweet_map.len().try_into().unwrap());
    bar.set_style(
        ProgressStyle::with_template(
            "Write file: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} [{eta_precise}]",
        )
        .unwrap(),
    );
    writer.write_record(&["id", "last_timestamp"]).unwrap();
    for (key, item) in tweet_map {
        writer
            .write_record(&[key.to_string(), item.to_string()])
            .unwrap();
        bar.inc(1);
    }
    bar.finish()
}
