"""
Author: Joshua Ashkinaze

Description: Pulls Twitter data for the selected panel of followers.

USAGE NOTES
There are two jsonl files that are created.
- The first one is called `{fn}_raw.jsonl` and contains the raw data from the Twitter API. The keys are:
    `original_user_id`, `data`, `includes_users`, and `includes_tweets` where the latter three are lists of dicts. To explain:
    When you query Tweepy for a list of tweets and add expansions, then `data` is the primary tweet data and includes_users
    and includes_tweets are the data for the users and tweets that are referenced in the primary tweet data.

- The second is called `{fn}_procd.jsonl` and contains the processed data. The keys are: `original_user_id` and `procd`
    where `procd` is a list of dicts. The difference between the two is that the `procd` data has been parsed in two key ways.
    First, I collect all the URLs in a nice expanded format (for primary and refd). Second, I also add the author info
    to the data for each ref tweet.

MISSING DATA
- If there are errors then we still write the data to the file, but we write -1 for keys other than `original_user_id`
- If the user actually has no tweets then we write -9 instead of -1

Date: 2024-04-03 10:02:23
"""

import pandas as pd
import numpy as np
import random
import json
import tweepy
import argparse
import logging

random.seed(416)
np.random.seed(416)

with open('twitter_creds3.json', 'r') as file:
    secrets = json.load(file)
TWITTER_API = secrets['personal_news']
client = tweepy.Client(bearer_token=TWITTER_API['bearer_token'], wait_on_rate_limit=True)


def tweet_controller(user_ids, n, fn):
    with open(f'{fn}_raw.jsonl', 'w') as raw_file, open(f'{fn}_procd.jsonl', 'w') as procd_file:

        user_id_no_tweets =0
        user_id_errors = 0
        user_id_success = 0
        counter = 0

        for user_id in user_ids:
            # Get data --> -1 if error
            try:
                tweets = get_tweets(user_id, n)
                if tweets.data:
                    raw, procd = process_tweets(tweets, user_id)
                    user_id_success += 1
                else:
                    raw = {'original_user_id': user_id, 'data': -9, 'includes_users': -1, 'includes_tweets': -9}
                    procd = {'original_user_id': user_id, 'procd': -9}
                    user_id_no_tweets += 1
            except Exception as e:
                logging.info(f"Error for user {user_id}: ")
                logging.info(f"Error: {e}")
                raw = {'original_user_id': user_id, 'data': -1, 'includes_users': -1, 'includes_tweets': -1}
                procd = {'original_user_id': user_id, 'procd': -1}
                user_id_errors += 1

                # write data
            raw_file.write(json.dumps(raw) + "\n")
            procd_file.write(json.dumps(procd) + "\n")

            counter += 1
            if counter % 50 == 0:
                logging.info(f"Finished {counter} of {len(user_ids)}")

    logging.info("Done")
    logging.info(f"Errors: {user_id_errors}")
    logging.info(f"Success: {user_id_success}")
    logging.info(f"No tweets: {user_id_no_tweets}")


def get_tweets(user_id, n=10):
    tweets_response = client.get_users_tweets(
        id=user_id,
        max_results=n,
        tweet_fields=[
            "attachments", "author_id", "conversation_id",
            "created_at", "entities", "geo", "id", "in_reply_to_user_id", "lang", "public_metrics", "referenced_tweets",
            "reply_settings",
            "source", "text", "withheld", "note_tweet"
        ],
        media_fields=['url', 'preview_image_url'],
        expansions=[
            "attachments.poll_ids", "attachments.media_keys", "author_id", "geo.place_id",
            "in_reply_to_user_id", "referenced_tweets.id", "entities.mentions.username",
            "referenced_tweets.id.author_id",
        ]
    )
    return tweets_response


def process_tweets(tweets_response, user_id):
    try:
        includes_tweet_data = [tweets_response.includes['tweets'][i].data for i in
                               range(len(tweets_response.includes['tweets']))]
    except:
        includes_tweet_data = []

    try:
        includes_user_data = [tweets_response.includes['users'][i].data for i in
                              range(len(tweets_response.includes['users']))]
    except:
        includes_user_data = []

    basic_data = [tweets_response.data[i].data for i in range(len(tweets_response.data))]
    raw = {'original_user_id': user_id, 'data': basic_data, 'includes_users': includes_user_data,
           'includes_tweets': includes_tweet_data}

    procd = []
    for i in range(len(tweets_response.data)):
        procd.append(parse_tweet(tweets_response.data[i].data, includes_tweet_data, includes_user_data))
    procd = {'original_user_id': user_id, 'procd': procd}
    return raw, procd


def parse_tweet(tweet, includes_tweet_data, includes_user_data):
    # Init these empty lists
    tweet['primary_urls'] = []
    tweet['refd_urls'] = []
    tweet['all_urls'] = []

    # Try to get primary urls
    urls = tweet.get('entities', {}).get('urls', []) if 'entities' in tweet else []
    if urls:
        try:
            tweet['primary_urls'] = [x['expanded_url'] for x in urls]
        except:
            print(urls)

    # Try to get referenced tweets
    if 'referenced_tweets' not in tweet:
        tweet['referenced_tweets'] = []
    else:

        # We first find the tweet that the original tweet refers to.
        # Then we loop through `ref_tweets` to get the info for this tweet.
        ref_tweets = tweet['referenced_tweets']
        for ref_tweet in ref_tweets:
            ref_tweet['urls'] = []
            ref_tweet['ref_author_id'] = None
            ref_tweet['ref_author_username'] = None

            tweet_id = ref_tweet['id']

            # Get tweet data from the refd tweet
            for t in includes_tweet_data:
                if t['id'] == tweet_id:
                    try:
                        urls = t.get('entities', {}).get('urls', []) if 'entities' in t else []
                        expanded_urls = [url['expanded_url'] for url in urls if 'expanded_url' in url]
                        ref_tweet['urls'].extend(expanded_urls)
                        tweet['refd_urls'].extend(expanded_urls)
                        ref_tweet['ref_author_id'] = t['author_id']
                    except:
                        print(urls)
                        pass

            # Get author data from the retweet
            for t in includes_user_data:
                if t['id'] == ref_tweet['ref_author_id']:
                    ref_tweet['ref_author_username'] = t['username']
    tweet['all_urls'] = list(set(tweet['primary_urls'] + tweet['refd_urls']))

    return tweet


def main(n_per_user, file_prefix, debug):
    logging.basicConfig(filename=f"{file_prefix}_data.log", filemode='w', level=logging.INFO,
                        format='%(asctime)s: %(message)s', datefmt='%Y-%m-%d__%H--%M--%S')
    df = pd.read_csv("hydrated_users.csv", dtype={'id': str})
    if debug:
        df = df.sample(n=10)
    df = df.sample(n=10)
    user_ids = df['id'].tolist()
    tweet_controller(user_ids, n_per_user, file_prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get tweet data')
    parser.add_argument('-n_per_user', '--n_per_user', type=int, required=True, help='Number of items per user')
    parser.add_argument('-file_prefix', '--file_prefix', type=str, required=True, help='Prefix for the output file')
    parser.add_argument('-d', '--d', dest='debug', action='store_true', default=False,
                        help='Enable debug mode (default: False)')

    args = parser.parse_args()
    main(args.n_per_user, args.file_prefix, args.debug)
