# Description of files 


# `1_scrape_pf_links.py`
This file scrapes PolitiFact links. It forms the basis of misinformation. 


# `2_filter_pf_links.ipynb`
The first stage filters PolitiFact links. Filters are related to:
- truth values (only pants on fire, false, mostly false)
- date ranges: after 2021-06-01
- about health
- occured on X (which we try to assess by looking at the description of the rumor)

Then we manually searched for an associated Twitter account of each of the posts.

After annotating a CSV with associated Twitter account, we prioritized certain accounts by a combination of followers and amount of times they shared things. 

# `3_pull_init_followers.sh`
This script pulls the initial followers of the prioritized accounts. In the first pass, we pull a max of `n` 

# `4_assign_treat_control.ipynb`
This script first de-duplicates followers and then assigns followers to treatment or control. A dataframe that that is outputted is
`"treat_status_MINIMAL_FOLLOWERS_03.04.2024__17.11.03__START0_END-1.csv"
`

# `5_pow.ipynb`

Monte Carlo power analysis. We use this to determine the final N. The final N is less
than the number of followers we initially pull, so we will downsample later. 

# `6_downsized_assign_treat_control.ipynb`
Here we downsample the followers in accordance with the results of the power analysis in the previous step. The resultant file is
`final_treat_status_MINIMAL_FOLLOWERS_03.04.2024__17.11.03__START0_END-1.csv`


# `7_select_panel_followers.py`
In this script we aim to track 40 followers per spreader but one of the issues is that we do not know
which followers will have valid tweet data in the next step. So we add some padding and hydrate data for 40*4 followers and this results in
`oversample_hydrated_users.csv`

# `8_get_pre_tweet_data.sh`
This script fetches 20 tweets for 40 followers with valid tweets. We stop searching after hit 40. 
The bash call is `python3 get_tweet_data.py --fn 'oversample_hydrated_users.csv' --n_per_user 20 --n_users_per_spreader 40 --file_prefix "pre"`. The final list of of followers is
`pre_40_success.csv` and there `pre_raw.jsonl` and `pre_processed.jsonl` contain tweet data where
the latter has some light processing to it. 


