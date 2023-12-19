#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import gzip
import glob
import json

from datetime import datetime
from twitwi import normalize_tweet
from multiprocessing import Manager, Pool


def read_file(args):
    in_file, tweet_dict = args
    print(datetime.now(), in_file)
    with gzip.open(in_file, 'rb') as f:
        for i, line in enumerate(f):

            ts, _, tweet = json.loads(line.decode('utf-8')) # type: ignore

            if "id" in tweet:

                normalized = normalize_tweet(tweet, extract_referenced_tweets=True)
                ts = int(ts)

                for subtweet in normalized:
                    sub_id = int(subtweet["id"])

                    if sub_id in tweet_dict:

                        previous_ts = tweet_dict[sub_id]
                        if ts > previous_ts:
                            tweet_dict[sub_id] = ts
                    else:
                        tweet_dict[sub_id] = ts

            if i % 1000000 == 0:
                print(datetime.now(), in_file, round(100*i/8472140, 2), len(tweet_dict))

if __name__ == '__main__':
    files_path = sys.argv[1]
    nb_processes = int(sys.argv[2])

    with Manager() as manager:

        tweet_dict = manager.dict()

        with Pool(processes=nb_processes) as pool:
            pool.imap_unordered(read_file, ((file, tweet_dict) for file in sorted(glob.glob(files_path))))
            pool.close()
            pool.join()

            print(len(tweet_dict))
            with open("tweet_dict.csv", "w") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "last_timestamp"])
                for key, value in tweet_dict.items():
                    writer.writerow([key, value])

