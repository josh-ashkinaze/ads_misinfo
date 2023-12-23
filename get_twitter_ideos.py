"""
Author: Joshua Ashkinaze
Date: September 2022

Parallel/efficient merge of Twitter IDs with a file of Twitter IDs from Pablo Barbera. These twitter IDs are 64 million users following at least 3 political accounts
as of August 2020.

Note: Barbera's R script works even with accounts following less than 3 IDs so it is more expansive than this merge. However, (1) the R script is very slow for large amounts of users and
(2) the ideologies will be more accurate with more political accounts followed, anyway.

"""

from joblib import Parallel, delayed
import os
import logging
import argparse
import pandas as pd
from datetime import datetime


def merge_id_chunk(input_df, input_id_col, id_url, join_type):
    """
    Merges data from input file with a chunk of the ideology data pablo sent

    Args:
        input_df (df): Pandas dataframe
        input_id_col (str): String pointing to user id
        id_url (str): Google API url
        join_type (str): One of "inner", "outer"
    Returns:
        Inner join of input_fn on id chunk
    """
    id_df = pd.read_csv(id_url, dtype={"id_str": 'str'})
    if join_type == "left":
        return pd.merge(input_df, id_df, left_on=input_id_col, right_on="id_str", how='left', indicator=True)
    elif join_type == "inner":
        return pd.merge(input_df, id_df, left_on=input_id_col, right_on="id_str", how='inner', indicator=True)

    print("Done with chunk")


def clean_data(id_df, id_col):
    id_df = id_df.sort_values(by=[id_col, 'theta'])
    id_df["id_rank"] = id_df.groupby(id_col)["theta"].rank(method='first', na_option='bottom', ascending=False)
    fixed_data = id_df.query("id_rank==1")
    fixed_data['_merge'] = fixed_data['_merge'].replace({'left_only': 'missing', 'both': 'non_missing'})
    fixed_data = fixed_data.rename(columns={"_merge": "found_ideo"})
    return fixed_data


def main(input_fn, id_col, join_type, debug_mode, output_fn=None, client_df=None):
    debug_str = "DEBUG_" if debug_mode else ""
    fn = f"""IDEO_{debug_str}{join_type.upper()}_{datetime.today().strftime('%Y-%m-%d-%H:%M:%S')}"""
    logging.basicConfig(
        filename=f"""{fn}.log""",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    n_jobs = os.cpu_count() - 1

    logging.info(f"""Using {input_fn}, join type: {join_type},  n_jobs:{n_jobs}""")

    # Ideology chunks that pablo emailed me
    # These represent all people who followed at least 3 political accounts as of 2020
    i0 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000000"
    i1 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000001"
    i2 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000002"
    i3 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000003"
    i4 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000004"
    i5 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000005"
    i6 = "https://storage.googleapis.com/tweetscores/user-ideal-points-202008_000000000006"
    ids = [i0, i1, i2, i3, i4, i5, i6] if not debug_mode else [i0]

    # Input file
    if not client_df:
        df = pd.read_csv(input_fn, dtype={id_col: 'object'})
    else:
        df = client_df
    dfs = pd.concat(
        Parallel(n_jobs=n_jobs)(delayed(merge_id_chunk)(df, id_col, ids[x], join_type) for x in range(len(ids))))
    dfs = clean_data(dfs, id_col)

    # If output filename is provided, use it. Otherwise, use the generated filename.
    output_fn = output_fn if output_fn else f"""{fn}.csv"""
    logging.info(f"""Wrote to file {output_fn}""")
    logging.info(f"""Original file was length {len(df)} and new file is of length {len(dfs)}""")
    dfs.to_csv(output_fn)
    return dfs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-input_fn", "-i", required=True, help="Input filename")
    parser.add_argument("-id_col", "-c", required=True, help="Column pointing to user id")
    parser.add_argument("--join_type", "--j", help="Join type (defaults to inner)", choices=['inner', 'left'],
                        default='inner')
    parser.add_argument("--debug", "--d", help="Only runs with first ID chunk for testing",
                        action='store_true')
    parser.add_argument("--output_fn", "--o", help="Optional output filename",
                        default=None)
    args = parser.parse_args()
    main(input_fn=args.input_fn, id_col=args.id_col, join_type=args.join_type, debug_mode=args.debug, output_fn=args.output_fn)
