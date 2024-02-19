"""
Author: Joshua Ashkinaze
Date: 01/17/2023

Description: This script is used to get the followers and friends of a list of users.
It uses multithreading to speed up the process and a rotating set of API keys to avoid rate limits.

Command-line python script to batch download followers or friends, using a rotated list of APIs.



Updated 2023-12-23:
- Due to Twitter API going down, no longer uses multithreading---just the one account that still works
- Added a flag called max_pull which will pull a max of this many friends or followers
- Changed get_people_relation_minimal so it works with usernames and not user ids 
- Depracated get_people_relation



"""

import argparse
import csv
import logging
import threading
import pandas as pd
import tweepy

from helpers import dt_str, exception2value, return_api_dict
import os

import math

def get_people_relations(api, user_list, relation_type, output_fn, account_name, is_minimal, max_pull):
    """
    Manages the people relations function calls. Gets either friends or followers for a list of users,
    rotating different API keys.

    Args:
        api: Tweepy api
        user_list: A list of user_ids
        relation_type: {'friends', 'followers'}
        output_fn: Str of csv name, matches name of log

    Returns:
        None, writes to a CSV file named `output_fn`. Each row of CSV has these fields:
        ['main',
        '{relation_type}_username',
        '{relation_type}_id',
        '{relation_type}_followers',
        '{relation_type}_following',
        '{relation_type}_tweet_count',
        '{relation_type}_created_date']
    """
    counter = 0
    user_list = user_list
    len_users = len(user_list)

    with open(f"""{output_fn}.csv""", "w") as f:
        if not is_minimal:
            fieldnames = ['main', f"""{relation_type}_username""", f"""{relation_type}_id""",
                          f"""{relation_type}_followers""", f"""{relation_type}_following""",
                          f"""{relation_type}_tweet_count""", f"""{relation_type}_created_date"""]
            api_connection = 'client'
            target_func = get_follow_relation
        else:
            fieldnames = ['main', f"""{relation_type}_id"""]
            api_connection = 'api'
            target_func = get_follow_relation_minimal

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for user in user_list:
            logging.info(f"""Parsing {counter} of {len_users} with {account_name}""")
            people = target_func(account_name, api[api_connection], user, relation_type, max_pull)
            for f in people:
                writer.writerow(f)
            counter += 1


def get_follow_relation(account_name, client, user_id, relation_type, max_pull):
    """
    Get either friends or followers of a given user.

    Args:
        relation_type: Either "friend" or "follower"
        client: Tweepy client
        user_id: id of twitter user

    Returns:
        List of dict with keys:
            ['main',
            '{relation_type}_username',
            '{relation_type}_id',
            '{relation_type}_followers',
            '{relation_type}_following',
            '{relation_type}_tweet_count',
            '{relation_type}_created_date']

        where main corresponds to the seed and the other columns are attributes of a particular friend or follower
    """
    # List to hold many people data
    raise Exception("Deprecated")
    people = []

    # List to hold minimal people fields, for writing to log in case CSV file gets messed up
    min_people = []

    func_call = client.get_users_followers if relation_type == "followers" else client.get_users_following

    try:
        for f in tweepy.Paginator(func_call, id=user_id, user_fields=['public_metrics', 'created_at'],
                                  max_results=1000).flatten(
        ):
            if len(people) >= max_people:
                break 
            else:
                pass                 
            data = dict.fromkeys(
                ['main', f"""{relation_type}_username""", f"""{relation_type}_id""", f"""{relation_type}_followers""",
                 f"""{relation_type}_following""",
                 f"""{relation_type}_tweet_count""", f"""{relation_type}_created_date"""], -1)
            data.update({"main": user_id})
            try:
                data.update({f"""{relation_type}_username""": f.data['username']})
                data.update({f"""{relation_type}_id""": f.data['id']})
                data.update({f"""{relation_type}_followers""": f.data['public_metrics']['followers_count']})
                data.update({f"""{relation_type}_following""": f.data['public_metrics']['following_count']})
                data.update({f"""{relation_type}_tweet_count""": f.data['public_metrics']['tweet_count']})
                data.update({f"""{relation_type}_created_date""": f.data['created_at']})
                people.append(data)
                min_people.append({'main': user_id, f"""{relation_type}_id""": data[f"""{relation_type}_id"""]})
            except Exception as e:
                logging.info(f"""ERROR parsing data for {user_id}:{e}""")
        # If no followers return -99
        if not people:
            data = dict.fromkeys(
                ['main', f"""{relation_type}_username""", f"""{relation_type}_id""", f"""{relation_type}_followers""",
                 f"""{relation_type}_following""",
                 f"""{relation_type}_tweet_count""", f"""{relation_type}_created_date"""], -99)
            data.update({"main": user_id})
            return [data]

    # If can't pull data return error
    except Exception as e:
        logging.info(f"""ERROR: Couldn't pull any data for {user_id}: {e}""")
        data = dict.fromkeys(
            ['main', f"""{relation_type}_username""", f"""{relation_type}_id""", f"""{relation_type}_followers""",
             f"""{relation_type}_following""",
             f"""{relation_type}_tweet_count""", f"""{relation_type}_created_date"""], e)
        data.update({"main": user_id})
        return [data]

    logging.info(f"""{str(len(people))} people pulled for this user""")
    logging.info(min_people)
    return people


def get_follow_relation_minimal(account_name, api, user_id, relation_type, max_pull):
    """Get either friends or followers of a given user.

    Args:
        account_name: Name of account
        api: An instance of the `tweepy.API` class.
        user_id: The ID of the user.
        relation_type: The type of relation to get, either 'friends' or 'followers'.
        max_pull: stop after this n

    Returns:
        A list of dictionaries containing the `main` user ID and the IDs of the
        `friends` or `followers` of the `main` user. If no friends or followers
        are found, returns a list with a single dictionary containing the `main`
        user ID and the value '00'. If an exception is raised when trying to
        retrieve the data for a particular user, the dictionary will contain the
        `main` user ID and the value '-1_<api_code>'. If an exception is raised
        when trying to retrieve data for any user, the function returns a list
        with a single dictionary containing the `main` user ID and the value
        '-99_<api_code>'.
    """
    # List to hold many people data
    people = []
    keys = ['main', f'{relation_type}_id']

    if relation_type == 'followers':
        func_call = api.get_follower_ids
    elif relation_type == 'friends':
        func_call = api.get_friend_ids
    else:
        raise "Entered a bad value: Needs to be one of ['followers', 'friends']"

    # Try to fetch the followers for a given user
    # If the initial function call does not throw an error, we enter the `try` block
    try:
        for f in tweepy.Cursor(func_call, stringify_ids=True, screen_name=user_id, count=5000).items():
            if len(people) >= max_pull:
                break 

            data = dict.fromkeys(keys, str(-999))
            data.update({'main': user_id})

            # Catch exceptions at the follower level, return API "-1_{api_code}"
            try:
                data.update({f'{relation_type}_id': str(f)})
                people.append(data)
            except Exception as e:
                logging.info(f'ERROR parsing data for particular {user_id}: {e}')
                data.update({f'{relation_type}_id': exception2value(e, '-1')})
                people.append(data)

        # Return "00" if no followers/friends
        if len(people) == 0:
            people = [{'main': user_id, f'{relation_type}_id': '00'}]

    # If the function call DOES throw an error, then we can't get any data for a main account.
    # If can't pull ANY data for user (i.e: exception at the main account level) return "-99_{api code}"
    except Exception as e:

        logging.info(f"ERROR: Couldn't pull any data for {user_id}: {e}")
        people = [{'main': user_id, f"""{relation_type}_id""": f"""{exception2value(e, "-99")}"""}]
    logging.info("Logged {} people with {}".format(len(people), account_name))
    logging.info(people)
    return people


def chunk_list(lst, k):
    """Split a list into k chunks."""
    n = len(lst)
    chunk_size = math.ceil(n/k)
    chunks = [lst[i:i+chunk_size] for i in range(0, n, chunk_size)]
    return chunks


def main(output_fn, input_fn, creds_fn, relation_type, is_minimal, start_idx, end_idx, max_pull):
    # Get ids
    f = open(input_fn)

    input_ids = [x.strip() for x in f.readlines()]
    input_ids = input_ids[start_idx:len(input_ids) if end_idx == -1 else end_idx]

    if max_pull == -1:
        max_pull = 999*1000*1000

    # Load twitter creds
    apis_dict = return_api_dict(creds_fn)

    # Chunk the list of ids

    # In the normal case the number of len(ids) >= len(apis) we break these ids into chunks
    if len(input_ids) >= len(apis_dict):
        chunks = chunk_list(input_ids, len(apis_dict))

    # But if debugging, we may have fewer ids than apis, so we just use an array of length 1 with the ids as the only element
    # And also, change api_dict so that it only has one api
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

    logging.info(f"""INPUT:{input_fn}, CREDS:{creds_fn}, "RELATION":{relation_type}, START:END={start_idx}:{end_idx}""")

    # Create threads
    threads = []
    for i in range(len(chunks)):
        account_name = list(apis_dict.keys())[i]
        t = threading.Thread(target=get_people_relations, args=(apis_dict[account_name], chunks[i], relation_type, output_fn + "_" + account_name, account_name, is_minimal, max_pull))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # # Get all files in directory

    # # Directory
    # path = output_fn.split("__")[0]

    # # Filename
    # suffix =output_fn.split("__")[1]
    # files = os.listdir(path)

    # # Filter out files that don't have the desired name pattern
    # desired_files = [file for file in files if suffix in file and file.endswith(".csv")]

    # # Read the csv files into dataframes
    # dfs = [pd.read_csv(file, dtype=object) for file in desired_files]
    # print(dfs)

    # # Concatenate all the dataframes into one
    # merged_df = pd.concat(dfs)
    # merged_df.to_csv(output_fn + "_merged" + ".csv")

    logging.info("ALL DONE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-input_fn', '-i', required=True, dest="input_fn",
                        help='Input filename, a list of IDs in a text file')
    parser.add_argument('-creds_fn', '-c', required=True, dest="creds_fn", help='Filename of credentials')
    parser.add_argument('-relation_type', '-r', required=True, dest="relation_type", help='friends or followers',
                        choices=['friends', 'followers'])
    parser.add_argument('-prefix', '-p', dest="prefix", help="Prefix for filename", default="")
    parser.add_argument('-start_idx', '-s', dest="start_idx", help="Index to start at default = 0", default=0, type=int)
    parser.add_argument('-end_idx', '-e', dest="end_idx", help="Index to end at, default = end of input file",
                        default=-1, type=int)
    parser.add_argument('-max_pull', '-mx', dest="max_pull", help="Get max per follower or friend. If -1 then ignore.",
                        default=50000, type=int)
    parser.add_argument('--debug', '-d', dest="debug", help="Change end_idx to 1", action='store_true')
    parser.add_argument('--minimal', '-m', dest="minimal",
                        help="If minimal, use v1 endpoint that only returns ids and not any user data. This endpoint returns 5000 ids per request rather than 1500.",
                        action='store_true')

    args = parser.parse_args()
    logging.info(f"ARGS: {args}")
    # If debug mode only get 1 user
    end_idx = 1 if args.debug else args.end_idx
    debug_tag = "DEBUG_" if args.debug else ""
    minimal_tag = "MINIMAL_" if args.minimal else ""

    prefix_tag = args.prefix + "__" if args.prefix else args.prefix
    output_fn = f"""{prefix_tag}{debug_tag}{minimal_tag}{args.relation_type.upper()}_{dt_str()}__START{args.start_idx}_END{end_idx}"""

    main(output_fn=output_fn, input_fn=args.input_fn, creds_fn=args.creds_fn, relation_type=args.relation_type,
         start_idx=args.start_idx, end_idx=end_idx, is_minimal=args.minimal, max_pull=args.max_pull)
