import collections
import json
from datetime import datetime

import tweepy


def return_api_dict(creds_fn, auth_type='app'):
    """
    Returns a nested dictionary of tweepy instances

    Args:
        creds_fn: A json file of user credentials
        auth_type: App or user authentication

    Returns:
        Dictionary of Tweepy clients keyed by account alias
    """
    f = open(creds_fn)
    api_dict = json.load(f)
    tweepy_dict = collections.defaultdict(dict)
    for key in api_dict.keys():
        bearer_token = api_dict[key]['bearer_token']
        if auth_type == 'app':
            auth = tweepy.OAuth2BearerHandler(bearer_token)
        else:
            auth = tweepy.OAuth1UserHandler(
                api_dict[key]['api_key'], api_dict[key]['api_key_secret'],
                api_dict[key]['access_token'], api_dict[key]['access_token_secret']
            )
        tweepy_dict[key]['client'] = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
        tweepy_dict[key]['api'] = tweepy.API(auth, wait_on_rate_limit=True, timeout=1200, retry_count=10, retry_delay=5,
                                             retry_errors=set([500, 502, 503, 504]))
    return tweepy_dict


def dt_str():
    """
    Return datetime string
    """
    now = datetime.now()  # current date and time
    date_time = now.strftime("%m.%d.%Y::%H.%M.%S")
    return date_time


def exception2value(e, prefix=""):
    """
    Attempts to return the twitter server error by parsing tweepy errors.

    If not a tweepy error just return the normal error

    """
    if isinstance(e, tweepy.errors.TweepyException):
        try:
            ec = str(-1 * int(e.response.status_code))
        except:
            ec = str(e.response)
    else:
        ec = str(-99)

    if prefix:
        return f"""{prefix}_{ec}"""
    else:
        return ec


def chunk_list(L, n):
    # Initialize an empty list to store the chunks
    chunks = []
    # Iterate over the list, `L`
    for i in range(0, len(L), n):
        # Get the chunk of maximum length `n`
        chunk = L[i:i + n]
        # Append the chunk to the list of chunks
        chunks.append(chunk)
    # Return the list of chunks
    return chunks
