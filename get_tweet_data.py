"""
Author: Joshua Ashkinaze

Description: Pulls Twitter data for the selected panel of followers, getting `n_user_per_spreader` for (condition, spreader) where we only
get users who have tweets. Or if `n_users_per_spreader' not provided then it simply gets all user ids in the df.

USAGE NOTES
There are two jsonl files that are created.
- The first one is called `{fn}_raw.jsonl` and contains the raw data from the Twitter API. The keys are:
    `original_user_id`, `data`, `includes_users`, and `includes_tweets` where the latter three are lists of dicts. To explain:
    When you query Tweepy for a list of tweets and add expansions, then `data` is the primary tweet data and includes_users
    and includes_tweets are the data for the users and tweets that are referenced in the primary tweet data.

- The second is called `{fn}_processed.jsonl` and contains the processed data. The keys are: `original_user_id` and `processed`
    where `processed` is a list of dicts. The difference between the two is that the `processed` data has been parsed in two key ways.
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
import datetime
import csv

random.seed(416)
np.random.seed(416)

with open('twitter_creds3.json', 'r') as file:
    secrets = json.load(file)
TWITTER_API = secrets['personal_news']
client = tweepy.Client(bearer_token=TWITTER_API['bearer_token'], wait_on_rate_limit=True)


def fetch_and_process_tweets(user_id, n_per_user):
    try:
        tweets = get_tweets(user_id, n_per_user)
        if tweets.data:
            raw, processed = process_tweets(tweets, user_id)
        else:
            raw = {'original_user_id': user_id, 'data': -9, 'includes_users': -9, 'includes_tweets': -9}
            processed = {'original_user_id': user_id, 'processed': -9}
    except Exception as e:
        logging.exception(f"Error for user {user_id}: {e}")
        raw = {'original_user_id': user_id, 'data': -1, 'includes_users': -1, 'includes_tweets': -1}
        processed = {'original_user_id': user_id, 'processed': -1}

    return raw, processed


def write_to_files(raw_file, processed_file, raw, processed):
    raw_file.write(json.dumps(raw) + "\n")
    processed_file.write(json.dumps(processed) + "\n")


def tweet_controller_ids(df, n_per_user, fn):
    with open(f'{fn}_raw.jsonl', 'w') as raw_file, open(f'{fn}_processed.jsonl', 'w') as processed_file:
        for user_id in df['id']:
            raw, processed = fetch_and_process_tweets(user_id, n_per_user)
            write_to_files(raw_file, processed_file, raw, processed)
    logging.info("Done")


def tweet_controller(df, n_per_user, n_users_per_spreader, fn):
    with open(f'{fn}_raw.jsonl', 'w') as raw_file, open(f'{fn}_processed.jsonl', 'w') as processed_file, open(
            f'{fn}_{n_users_per_spreader}_success.csv', 'w', newline='') as success_file:
        csv_writer = csv.writer(success_file)
        csv_writer.writerow(['follower_id', 'spreader_username', 'condition'])

        for (spreader_username, condition), group in df.groupby(['spreader_username', 'condition']):
            user_ids = group['id'].tolist()
            user_id_success = 0

            for user_id in user_ids:
                if user_id_success >= n_users_per_spreader:
                    break
                raw, processed = fetch_and_process_tweets(user_id, n_per_user)
                if raw['data'] != -1 and raw['data'] != -9:
                    write_to_files(raw_file, processed_file, raw, processed)
                    csv_writer.writerow([user_id, spreader_username, condition])
                    user_id_success += 1
            logging.info("Finished a spreader block")
            logging.info("Success {}".format(user_id_success))

    logging.info("Done with all users")


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
    # Try to get included tweets unless there are no included tweets bundled
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

    # Write this raw data to a dict
    basic_data = [tweets_response.data[i].data for i in range(len(tweets_response.data))]
    raw = {'original_user_id': user_id, 'data': basic_data, 'includes_users': includes_user_data,
           'includes_tweets': includes_tweet_data}

    # Do some processing
    processed = []
    for i in range(len(tweets_response.data)):
        try:
            parsed = parse_tweet(tweets_response.data[i].data, includes_tweet_data, includes_user_data)
        except:
            parsed = -1
        processed.append(parsed)

    processed = {'original_user_id': user_id, 'processed': processed}
    return raw, processed


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
            for included_tweet in includes_tweet_data:
                if included_tweet['id'] == tweet_id:

                    # Try to get refd tweet urls except pass
                    try:
                        urls = included_tweet.get('entities', {}).get('urls',
                                                                      []) if 'entities' in included_tweet else []
                        expanded_urls = [url['expanded_url'] for url in urls if 'expanded_url' in url]
                        ref_tweet['urls'].extend(expanded_urls)
                        tweet['refd_urls'].extend(expanded_urls)
                    except:
                        pass

                    # Try to get refd tweet author id except pass
                    try:
                        ref_tweet['ref_author_id'] = included_tweet['author_id']

                        # Now if got the author id, we can get the author username from red tweets too
                        for included_author in includes_user_data:
                            if included_author['id'] == ref_tweet['ref_author_id']:
                                ref_tweet['ref_author_username'] = included_author['username']
                    except:
                        pass

    tweet['all_urls'] = list(set(tweet['primary_urls'] + tweet['refd_urls']))

    return tweet


def main(fn, n_per_user, n_users_per_spreader, file_prefix, debug):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d__%H--%M--%S')
    file_prefix = f"{file_prefix}_{timestamp}"

    logging.basicConfig(filename=f"{file_prefix}_data.log", filemode='w', level=logging.INFO,
                        format='%(asctime)s: %(message)s', datefmt='%Y-%m-%d__%H--%M--%S')
    df = pd.read_csv("hydrated_users.csv", dtype={'id': str})
    df = df.sample(frac=1, random_state=42)
    if n_users_per_spreader:
        tweet_controller(df, n_per_user, n_users_per_spreader, file_prefix)
    else:
        tweet_controller_ids(df, n_per_user, file_prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get tweet data')
    parser.add_argument('-fn', '--fn', type=str, required=True, help='csv file with column `id`')
    parser.add_argument('-n_users_per_spreader', '--n_users_per_spreader', type=int, required=False,
                        help='Number of users with valid tweets to pull')
    parser.add_argument('-n_per_user', '--n_per_user', type=int, required=True, help='Number of items per user')
    parser.add_argument('-file_prefix', '--file_prefix', type=str, required=True, help='Prefix for the output file')
    parser.add_argument('-d', '--d', dest='debug', action='store_true', default=False,
                        help='Enable debug mode (default: False)')

    args = parser.parse_args()
    main(args.fn, args.n_per_user, args.n_users_per_spreader, args.file_prefix, args.debug)
