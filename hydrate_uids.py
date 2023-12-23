"""
Author: Joshua Ashkinaze
Date: 01/17/2023

Description: This script takes a list of Twitter user IDs and returns a list of dictionaries containing information
about the users. It uses multiple Twitter API accounts to speed up the process.  The script will automatically retry
failed requests.

usage: hydrate_uids.py [-h] -input_fn INPUT_FN -creds_fn CREDS_FN [-prefix PREFIX] [-start_idx START_IDX] [-end_idx END_IDX] [-pandas_column PANDAS_COLUMN] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  -input_fn INPUT_FN, -i INPUT_FN
                        Input filename, a list of IDs in a text file
  -creds_fn CREDS_FN, -c CREDS_FN
                        Filename of credentials
  -prefix PREFIX, -p PREFIX
                        Prefix for filename
  -start_idx START_IDX, -s START_IDX
                        Index to start at default = 0
  -end_idx END_IDX, -e END_IDX
                        Index to end at, default = end of input file
  -pandas_column PANDAS_COLUMN, -pc PANDAS_COLUMN
                        Read id column from a Pandas dataframe
  --debug, -d           Change end_idx to 1

"""

import argparse
import csv
import logging
import math
import os
import threading

import pandas as pd

from helpers import dt_str, return_api_dict


def hydrate_user_chunks(api, account_name, user_ids, output_fn):
    """
    Hydrate a list of Twitter user IDs and write the results to a CSV file.

    Args:
        api: A Twitter API object.
        account_name: The name of the Twitter account associated with the API object.
        user_ids: A list of Twitter user IDs.
        output_fn: The name of the output file.

    Returns:
        None
    """
    counter = 0
    chunks = chunk_list_size_n(user_ids, 100)
    chunks = [x for x in chunks]
    len_chunks = len(chunks)

    with open(f"""{output_fn}.csv""", "w") as f:
        fieldnames = ['user_id',
                      'username',
                      'follower_count',
                      'following_count',
                      'tweet_count',
                      'account_created',
                      'last_tweet_date',
                      'name',
                      'lang',
                      'last_tweet_id']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for chunk in chunks:
            logging.info(f"""Parsing {counter} of {len_chunks} with {account_name}""")
            chunk_processed = process_chunk(api['api'], chunk)
            if chunk_processed:
                for p in chunk_processed:
                    writer.writerow(p)
            counter += 1


def process_chunk(api: object, chunk: list, max_attempts=4) -> list:
    """Process a chunk of Twitter user IDs and return a list of dictionaries containing user information.

    Args:
        api: A Twitter API object.
        chunk: A list of Twitter user IDs.
        max_attempts: Max attempts

    Returns:
        A list of dictionaries containing information about the users in the chunk. If a user ID in the chunk is not returned by the 'lookup_users'
        method of the 'api' object, a dictionary with the 'user_id' set to the ID and all other values
        set to -9 is appended to the list.
    """
    attempts = 0
    while attempts < max_attempts:
        try:
            hydrated = api.lookup_users(user_id=chunk)
            processed = [process_user(x) for x in hydrated]
            pulled_uids = set([processed[i]['user_id'] for i in range(len(processed))])
            input_uids = set(chunk)
            not_pulled_uids = input_uids - pulled_uids
            if not_pulled_uids:
                for uid in not_pulled_uids:
                    keys = ['user_id',
                            'username',
                            'follower_count',
                            'following_count',
                            'tweet_count',
                            'account_created',
                            'last_tweet_date',
                            'name',
                            'lang',
                            'last_tweet_id']
                    t_dict = dict.fromkeys(keys, -9)
                    t_dict['user_id'] = str(uid)
                    processed.append(t_dict)
            return processed
        except Exception as e:
            logging.info("Ran into an error: {} and trying {} out of {} times".format(e, attempts, max_attempts))
            attempts += 1
            continue


def process_user(t_user: object) -> dict:
    """Process a Twitter user object and return a dictionary of user information.

    Args:
        t_user: A Twitter user object.

    Returns:
        A dictionary containing information about the user. If an exception occurs during the processing of the user object, the
        function returns a dictionary with all values set to -1.
    """
    keys = ['user_id',
            'username',
            'follower_count',
            'following_count',
            'tweet_count',
            'account_created',
            'last_tweet_date',
            'name',
            'lang',
            'last_tweet_id']
    t_dict = dict.fromkeys(keys, -1)
    try:
        t_user = t_user._json
        t_dict['user_id'] = t_user['id_str']
        t_dict['name'] = t_user['name']
        t_dict['username'] = t_user['screen_name']
        t_dict['account_created'] = t_user['created_at']
        t_dict['follower_count'] = t_user['followers_count']
        t_dict['following_count'] = t_user['friends_count']
        t_dict['tweet_count'] = t_user['statuses_count']
        t_dict['lang'] = t_user['lang']
        if "status" in t_user.keys():
            t_dict['last_tweet_date'] = t_user['status']['created_at']
            t_dict['last_tweet_id'] = t_user['status']['id_str']
        return t_dict
    except:
        return t_dict


def chunk_list_size_n(mylist, n):
    """Return a list of lists of size n."""
    for i in range(0, len(mylist), n):
        yield mylist[i:i + n]

def chunk_list(lst, k):
    """Split a list into k chunks.
    If the list cannot be split evenly, the last chunk will be smaller than the others.
    """
    n = len(lst)
    chunk_size = math.ceil(n / k)
    chunks = [lst[i:i + chunk_size] for i in range(0, n, chunk_size)]
    return chunks


def main(output_fn, input_fn, creds_fn, start_idx, end_idx, pandas_column):

    if not pandas_column:
        f = open(input_fn)
        input_ids = sorted(list(set([x.strip() for x in f.readlines()])))
    else:
        d = pd.read_csv(input_fn, dtype={pandas_column: 'object'})
        input_ids = sorted(list(set(d[pandas_column].tolist())))

    if end_idx != -1:
        input_ids = input_ids[start_idx:end_idx]
    else:
        input_ids = input_ids[start_idx:]

    # Load twitter creds
    apis_dict = return_api_dict(creds_fn, auth_type='user')

    # In the normal case the number of ids is >= apis_dict so we break these into chunks
    if len(input_ids) >= len(apis_dict):
        chunks = chunk_list(input_ids, len(apis_dict))

    # But if debugging, we may have fewer ids than apis_dict, so we just use the number of ids
    # and only one API
    else:
        first_element_key = list(apis_dict.keys())[0]
        apis_dict = {first_element_key: apis_dict[first_element_key]}
        chunks = [input_ids]

    # Log data
    logging.basicConfig(
        filename=f"""{output_fn}.log""",
        level=logging.INFO,
        format='%(asctime)s %(levelname)s (%(funcName)s:%(lineno)d): %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Create threads
    threads = []
    for i in range(len(chunks)):
        account_name = list(apis_dict.keys())[i]
        t = threading.Thread(target=hydrate_user_chunks,
                             args=(apis_dict[account_name], account_name, chunks[i], output_fn + "_" + account_name))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # Get all files in directory

    # Directory
    path = output_fn.split("Hydrated")[0]

    # Filename
    suffix = output_fn.split("__")[1]
    files = os.listdir(path + "/")

    # Filter out files that don't have the desired name pattern
    desired_files = [file for file in files if suffix in file and file.endswith(".csv")]

    # Read the csv files into dataframes
    dfs = [pd.read_csv(os.path.join(path, file), dtype={'username':'object', 'user_id':'object', 'last_tweet_id':'object'}) for file in desired_files]

    # Concatenate all the dataframes into one
    merged_df = pd.concat(dfs)
    merged_df.to_csv(output_fn + "_merged" + ".csv")

    logging.info("ALL DONE")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-input_fn', '-i', required=True, dest="input_fn",
                        help='Input filename, a list of IDs in a text file')
    parser.add_argument('-creds_fn', '-c', required=True, dest="creds_fn", help='Filename of credentials')
    parser.add_argument('-prefix', '-p', dest="prefix", help="Prefix for filename", default="")
    parser.add_argument('-start_idx', '-s', dest="start_idx", help="Index to start at default = 0", default=0, type=int)
    parser.add_argument('-end_idx', '-e', dest="end_idx", help="Index to end at, default = end of input file",
                        default=-1, type=int)
    parser.add_argument('-pandas_column', '-pc', dest="pandas_column", help="Read id column from a Pandas dataframe",
                        default="")
    parser.add_argument('--debug', '-d', dest="debug", help="Change end_idx to 1", action='store_true')
    args = parser.parse_args()
    # If debug mode only get 1 user
    end_idx = 1 if args.debug else args.end_idx
    debug_tag = "DEBUG_" if args.debug else ""

    prefix_tag = args.prefix + "__" if args.prefix else args.prefix
    output_fn = f"""{prefix_tag}{debug_tag}_{dt_str()}__START{args.start_idx}_END{end_idx}"""

    main(output_fn=output_fn, input_fn=args.input_fn, creds_fn=args.creds_fn,
         start_idx=args.start_idx, end_idx=end_idx, pandas_column=args.pandas_column)
