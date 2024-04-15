"""
Author: Joshua Ashkinaze

Description: Selects the panel of followers for which we pull pre/post Twitter data for. In this script, we
pull 3x as many followers than we need. This is because in a subsequent part we want to only pull data
for those followers who have tweets---so it's necessary to oversample in the first part.

Goal is to have 40 followers for each (spreader, treatment) block.

Date: 2024-04-02 13:32:47
"""

import pandas as pd
import tweepy
import json
import random
import numpy as np
import logging
import os

logging.basicConfig(filename=f"{os.path.splitext(os.path.basename(__file__))[0]}.log",
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s: %(message)s', datefmt='%Y-%m-%d__%H--%M--%S')
random.seed(42)
np.random.seed(42)

with open('twitter_creds3.json', 'r') as file:
    secrets = json.load(file)
TWITTER_API = secrets['personal_news']


def flatten_dict(d, parent_key='', sep='_'):
    """
    Flattens a dictionary, concatenating keys with a separator.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def return_tweepy_client(TWITTER_API):
    """
    Inits tweepy client
    """
    client = tweepy.Client(bearer_token=TWITTER_API['bearer_token'], wait_on_rate_limit=True)
    return client


def hydrate_users(client, master_df, panel_n, panel_n_pad):
    """
    Hydrates users, returning `panel_n' hydrated users per each of the (spreader, condition) blocks.

    Returns a DataFrame of hydrated users.


    """
    pull_n = panel_n + panel_n_pad
    hydrated_users = []

    for (spreader_username, condition), group in master_df.groupby(['spreader_username', 'condition']):
        n_available = len(group)
        sample_size = min(n_available, pull_n)
        sample = group.sample(n=sample_size, replace=False)

        user_ids = sample['follower_id'].tolist()

        logging.info(f"Hydrating followers for {spreader_username}, {condition}...")
        hydrated_count = 0
        index = 0

        while hydrated_count < pull_n and index < len(user_ids):
            try:
                batch = user_ids[index:index + 100]
                users = client.get_users(ids=batch, user_fields=['created_at', 'description',
                                                                 'protected',
                                                                 'username',
                                                                 'public_metrics',
                                                                 'location',
                                                                 'verified',
                                                                 'verified_type'
                                                                 ])
                flatten = [flatten_dict(user.data) for user in users.data]
                for f in flatten:
                    f['spreader_username'] = spreader_username
                    f['condition'] = condition
                    f['id'] = str(f['id'])
                hydrated_users.extend(flatten)
                hydrated_count += len(users)
                index += 100
            except Exception as e:
                logging.error(f"Error while hydrating {spreader_username}, {condition}: {e}")
                index += 100

        logging.info(f"Hydrated {hydrated_count} users for {spreader_username}, {condition}")
    logging.info("Hydration process completed.")
    hydrated_df = pd.DataFrame(hydrated_users)
    hydrated_df = hydrated_df.drop_duplicates(subset=['id'])
    return pd.DataFrame(hydrated_df)


def main():
    logging.info("Starting up")
    dfs = []
    usernames = ['charliekirk11', 'gatewaypundit', 'jackposobiec', 'realcandaceo', 'stkirsch']
    conditions = ['treat', 'ctrl']

    for username in usernames:
        for condition in conditions:
            df = pd.read_csv(f'final_{condition}_twit_{username}.txt', header=None)
            df['condition'] = condition
            df['spreader_username'] = username
            df.columns = ['follower_id', 'condition', 'spreader_username']
            dfs.append(df)

    master_df = pd.concat(dfs)
    client = return_tweepy_client(TWITTER_API)

    panel_n = 40
    panel_n_pad = int(panel_n*4)
    hydrated_panel = hydrate_users(client, master_df, panel_n, panel_n_pad)
    print(hydrated_panel.groupby(['spreader_username', 'condition']).size())
    print(hydrated_panel.head())

    hydrated_panel.to_csv("oversample_hydrated_users.csv", index=False)
    logging.info("Hydration process completed.")


if __name__ == '__main__':
    main()
