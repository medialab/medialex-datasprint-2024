#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import gzip
import glob
import json

from datetime import datetime
from multiprocessing import Pool


def read_file(in_file):
    print(datetime.now(), in_file)

    tweet_dict = {}
    with gzip.open(in_file, 'rb') as f:
        for i, line in enumerate(f):

            ts, _, tweet = json.loads(line.decode('utf-8')) # type: ignore

            if "id" in tweet:

                sub_ids = [int(tweet["id"])]
                ts = int(ts)

                if 'retweeted_status' in tweet and tweet['retweeted_status']['id_str'] != sub_ids[0]:
                    sub_ids.append(int(tweet['retweeted_status']['id_str']))

                elif 'quoted_status' in tweet and tweet['quoted_status']['id_str'] != sub_ids[0]:
                    sub_ids.append(int(tweet['quoted_status']['id_str']))

                for sub_id in sub_ids:
                    if sub_id in tweet_dict:

                        previous_ts = tweet_dict[sub_id]
                        if ts > previous_ts:
                            tweet_dict[sub_id] = ts
                    else:
                        tweet_dict[sub_id] = ts

            if i % 100000 == 0:
                print(datetime.now(), in_file, round(100*i/8472140, 2), len(tweet_dict))

    with open(in_file.replace(".gz", "_unique_ids.csv"), "w") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "last_timestamp"])
        for key, value in tweet_dict.items():
            writer.writerow([key, value])

if __name__ == '__main__':
    files_path = sys.argv[1]
    nb_processes = int(sys.argv[2])


    with Pool(processes=nb_processes) as pool:
        pool.imap_unordered(read_file, sorted(glob.glob(files_path)))
        pool.close()
        pool.join()
