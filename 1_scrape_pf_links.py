"""
Author: Joshua Ashkinaze
Date: 2023-12-22

Description: Scrape Politifact raw data. This gets links and some basic fields available without clicking each article
- 'type': type of post, but this field is messy
- 'date': date of statement,
- 'title': title of the statement,
- 'author': journalist writing politifact article,
- 'url': url of the page so can get more data ,
- 'is_twitter': helper column,
- 'raw_desc': the string description of the statement
- 'truth_value': truth value of the statement
"""

import argparse
import requests
import time
import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np
import logging
import os
from bs4 import BeautifulSoup

def convert_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return None

def categorize_where(desc):
    try:
        desc = desc.lower()
        if "twitter" in desc or "tweet" in desc or "post on x" in desc or "x-post" in desc or "x post" in desc or "en x":
            return 1
        else:
            return 0
    except:
        return np.NaN

def scrape_politifact(earliest_date, pause=2):
    page_start = 1
    scraped_data = []

    earliest_date = convert_date(earliest_date)
    if not earliest_date:
        raise ValueError("Invalid earliest date format. Use 'YYYY-MM-DD' format.")

    logging.info("Starting scraping process...")
    while True:
        try:
            page_url = f'https://www.politifact.com/factchecks/list/?page={page_start}'
            logging.info(f"Scraping page {page_start}...")
            response = requests.get(page_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            containers = soup.find_all("li", class_="o-listicle__item")

            if not containers:
                logging.info("No more data to scrape.")
                break

            for item in containers:
                meter_div = item.find("div", class_="m-statement__meter")
                truth_value_img = meter_div.find("img") if meter_div else None
                truth_value = truth_value_img['alt'] if truth_value_img else 'Truth value not found'
                footer = item.find("footer", class_="m-statement__footer")
                author, date_str = (footer.text.strip().split(" • ") if footer else ('Unknown', 'Unknown'))
                date_object = datetime.strptime(date_str, '%B %d, %Y')
                date = date_object.strftime('%Y-%m-%d')
                if date and date < str(earliest_date):
                    logging.info(f"Reached the earliest date ({earliest_date}). Stopping scraping.")
                    return pd.DataFrame(scraped_data)
                else:
                    pass

                quote_div = item.find("div", class_="m-statement__quote")
                url_anchor = quote_div.find("a", href=True) if quote_div else None
                url = url_anchor['href'] if url_anchor else 'URL not found'
                if url and not url.startswith('http'):
                    url = f"https://www.politifact.com{url}"

                type_anchor = item.find("a", class_="m-statement__name")
                title = quote_div.text.strip() if quote_div else 'Unknown'

                desc = item.find("div", class_="m-statement__desc").text.strip()

                scraped_info = {
                    'type': type_anchor.text.strip() if type_anchor else 'Unknown',
                    'date': date,
                    'title': title,
                    'author': author.replace("By ", ""),
                    'url': url,
                    'is_twitter': categorize_where(desc),
                    'raw_desc': desc,
                    'truth_value': truth_value
                }
                scraped_data.append(scraped_info)
            sleep_time = random.random()*pause
            time.sleep(sleep_time)
            page_start += 1

        except Exception as e:
            logging.info(f'Failed to scrape page {page_start}: {e}')

    df = pd.DataFrame(scraped_data)
    return df

def main():
    date_str = datetime.now().strftime("%Y-%m-%d__%H_%M_%S")

    LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
    logging.basicConfig(filename=f'{os.path.basename(__file__)}_{date_str}.log', level=logging.INFO, format=LOG_FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='w')
    parser = argparse.ArgumentParser(description="Scrape Politifact data.")
    parser.add_argument("--earliest_date", help="Earliest date for data in 'YYYY-MM-DD' format", nargs='?', default=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'))
    parser.add_argument("--fn", help="Filename to save the scraped data", default=None)
    parser.add_argument("--d", "--debug", help="Debug mode: scrape only until day before yesterday", action="store_true")
    args = parser.parse_args()

    if args.d:
        args.earliest_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

    try:
        datetime.strptime(args.earliest_date, '%Y-%m-%d')
    except ValueError:
        logging.error("Invalid earliest date format. Use 'YYYY-MM-DD' format.")
        return

    if not args.fn:
        args.fn = f'raw_pf_links_{date_str}.csv'

    logging.info("Starting scraping process with args: " + str(args))
    df = scrape_politifact(args.earliest_date)
    logging.info("Scraping complete.")
    df.to_csv(args.fn, index=False)

if __name__ == "__main__":
    main()