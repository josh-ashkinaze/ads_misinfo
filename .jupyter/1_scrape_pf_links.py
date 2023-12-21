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

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_date(date_str):
    """Converts a date string to a datetime object."""
    try:
        return datetime.strptime(date_str, '%B %d, %Y')
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

def scrape_politifact(earliest_date, filename=None, pause=2):
    page_start = 1
    scraped_data = []

    earliest_date = convert_date(earliest_date)
    if not earliest_date:
        raise ValueError("Invalid earliest date format. Use 'Month DD, YYYY' format.")

    logging.info("Starting scraping process...")
    while True:
        try:
            page_url = f'https://www.politifact.com/factchecks/list/?page={page_start}&ruling=false'
            logging.info(f"Scraping page {page_start}...")
            response = requests.get(page_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            containers = soup.find_all("li", class_="o-listicle__item")

            if not containers:
                logging.info("No more data to scrape.")
                break

            for item in containers:
                footer = item.find("footer", class_="m-statement__footer")
                author, date_str = (footer.text.strip().split(" • ") if footer else ('Unknown', 'Unknown'))
                date = convert_date(date_str)

                if date and date < earliest_date:
                    logging.info("Reached the earliest date. Stopping scraping.")
                    return pd.DataFrame(scraped_data)

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
                    'raw_desc': desc
                }
                scraped_data.append(scraped_info)
            sleep_time = random.random()*pause
            time.sleep(sleep_time)
            page_start += 1

        except Exception as e:
            logging.error(f'Failed to scrape page {page_start}: {e}')

    df = pd.DataFrame(scraped_data)
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")
    return df

def main():
    LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
    logging.basicConfig(filename=f'{os.path.basename(__file__)}.log', level=logging.INFO, format=LOG_FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='w')
    parser = argparse.ArgumentParser(description="Scrape Politifact data.")
    parser.add_argument("earliest_date", help="Earliest date for data in 'Month DD, YYYY' format", nargs='?', default=(datetime.now() - timedelta(days=2)).strftime('%B %d, %Y'))
    parser.add_argument("--fn", help="Filename to save the scraped data", default=f'raw_pf_links_{datetime.now().strftime("%Y%m%d.%H%M%S")}.csv')
    args = parser.parse_args()
    logging.info("Starting scraping process with args: " + str(args))
    df = scrape_politifact(args.earliest_date, filename=args.fn)
    logging.info("Scraping complete.")

if __name__ == "__main__":
    main()
