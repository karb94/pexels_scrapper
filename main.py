#! /bin/env python3

import pandas as pd
import logging
import sys
from pathlib import Path
import multiprocessing as mp

import pexels_scraper

format='%(asctime)s %(message)s'
datefmt='%Y/%m/%d %H:%M:%S'
logging.basicConfig(
    filename='pexels_scraper.log',
    encoding='utf-8',
    format=format,
    datefmt=datefmt,
    level=logging.INFO
)

def main():

    artists_urls_file = sys.argv[1] if len(sys.argv) > 1 else 'artists_urls.csv'
    data_file = sys.argv[2] if len(sys.argv) > 2 else 'data.csv'

    artists_urls = pd.read_csv(artists_urls_file, header=None, squeeze=True)
    drivers = pexels_scraper.create_drivers()

    logging.info('Starting Pexels web scraper')
    logging.info(f'Reading artists urls from "{artists_urls_file}"')
    logging.info(f'Data will be saved to "{data_file}"')
    logging.info(f'Using {mp.cpu_count()} CPU processor')

    def save_to_file(df, file_path):
        file = Path(file_path)
        header = not file.exists()
        df.to_csv(file, mode='a', header=header)

    for artist_url in artists_urls:
        df = pexels_scraper.get_artist_stats(artist_url, drivers) 
        save_to_file(df, data_file)



if __name__ == '__main__':
    main()

