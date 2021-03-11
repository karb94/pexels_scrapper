#! /bin/env python3

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException,
                                        TimeoutException, ElementClickInterceptedException)
import pandas as pd
import numpy as np
import datetime
import threading as t
from concurrent.futures import ThreadPoolExecutor
import psutil
from operator import methodcaller
from functools import partial, partialmethod
from itertools import chain
from itertools import chain
import math
import time
import sys
from pathlib import Path
import logging


logs_dir = Path('./logs')
logs_dir.mkdir(exist_ok=True)


def setup_logger(name):
    log_file = logs_dir / (name + '.log')
    handler = logging.FileHandler(log_file)
    logfmt = '[%(asctime)s] %(levelname)s: %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(logfmt, datefmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


n_physical_cores = psutil.cpu_count(logical=False)


def create_driver(logger):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = True
    # chrome_options.javascriptEnabled = True
    chrome_options.add_experimental_option(
        "excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-dev-shm-usage')
    # chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('window-size=1920,1080')
    chrome_options.add_argument('seleniumProtocol=WebDriver')
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")
    while True:
        try:
            driver = webdriver.Chrome(options=chrome_options)
            logger.info( 'Webdriver initialised correctly.')
            return driver
        except:
            logger.exception(
                'Web driver could not be initialised. Retrying...')

def vectorize(function):
    def wrapper(driver, logger, array):
        if not isinstance(array, np.ndarray):
            array = np.array(array, ndmin=1)
        while True:
            try:
                f = partial(function, driver, logger)
                break
            except TimeoutException:
                logger.exception('Timeout exception')
        return list(map(f, array))
    return wrapper


@vectorize
def get_collections_urls(driver, logger, artist_url):
    collections_url = artist_url + '/collections/'
    driver.get(collections_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    artist_name = driver.find_element_by_tag_name('h1').text
    logger.info(f'COLLECTIONS from "{artist_name}":')
    matches = soup.find_all(
        'a', {'class': 'discover__collections__collection'})

    def not_likes(collection): return 'likes' not in collection
    collections_dirs = list(
        filter(not_likes, map(methodcaller('get', 'href'), matches)))
    data = {
        'artist name': [artist_name] * len(collections_dirs),
        'collection url': collections_dirs
    }
    index = [artist_url] * len(collections_dirs)
    df = pd.DataFrame(data, index=index)
    df['collection url'] = 'https://www.pexels.com' + \
        df['collection url'].astype(str)
    return df


@vectorize
def get_content_urls(driver, logger, collection_url):
    driver.get(collection_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    collection_name = soup.find('h1').get_text().strip('\n')
    artist_name = driver.find_element_by_tag_name('span').text
    logger.info(
        f'FETCHING CONTENT from "{artist_name}" in "{collection_name}" collection')

    old_scroll_height = 0
    new_scroll_height = driver.execute_script(
        "return document.body.scrollHeight;")
    while old_scroll_height < new_scroll_height:
        old_scroll_height = new_scroll_height
        driver.execute_script(f"window.scrollTo(0, {old_scroll_height});")
        time.sleep(1)
        new_scroll_height = driver.execute_script(
            "return document.body.scrollHeight;")

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    video_class = 'js-photo-link js-photo-item__link photo-item__link'
    photo_class = 'js-photo-link photo-item__link'
    photos = soup.find_all('a', {'class': photo_class})
    videos = soup.find_all('a', {'class': video_class})
    content_dirs = list(
        map(methodcaller('get', 'href'), chain(photos, videos)))
    data = {
        'collection name': [collection_name] * len(content_dirs),
        'content url': content_dirs
    }
    index = [collection_url] * len(content_dirs)
    df = pd.DataFrame(data, index=index)
    df['content url'] = 'https://www.pexels.com' + \
        df['content url'].astype(str)
    logger.info(
        f'GOT CONTENT from "{artist_name}" in "{collection_name}" collection')
    return df


def to_number(string):
    d = {
        'K': 1000,
        'M': 1000000,
        'B': 1000000000
    }
    if string[-1] in list(d.keys()):
        key = string[-1]
        number = int(float(string.strip(key)) * d[key])
    else:
        number = int(string)
    return number


@vectorize
def get_content_stats(driver, logger, content_url):
    logger.info(f'SCRAPING stats from {content_url}')
    driver.get(content_url)
    xpath = {
        'button': '//*[@id="photo-page-body"]/div/div/section[1]/div[1]/button[2]',
        'title': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/h1/strong',
        'views': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[2]/div',
        'likes': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[2]/div',
        'downloads': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[1]/div',
        'upload date': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/small'
    }
    for i in range(3):
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath['button']))).click()
            WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located((By.XPATH, xpath['views'])))
            break
        except (ElementClickInterceptedException, TimeoutException):
            logger.warning(
                'Web driver timed out when looking '
                f'for the info button in {content_url}. Retrying...')
            driver.get('about:blank')
            time.sleep(2)
            driver.get(content_url)
            if i == 2:
                logger.warning(
                    f'{content_url} is corrupted. Assigning NA '
                    'values to this piece of content')
                data = {
                    'title': [np.nan],
                    'views': [np.nan],
                    'downloads': [np.nan],
                    'likes': [np.nan],
                    'upload date': [np.nan]
                }
                return pd.DataFrame(data, index=[content_url])

    def get_str_from_xpath(
        xpath): return driver.find_element_by_xpath(xpath).text
    def get_date(string): return datetime.datetime.strptime(
        string, "Uploaded at %B %d, %Y").strftime('%Y-%m-%d')
    try:
        title = driver.find_element_by_xpath(xpath['title']).text
    except NoSuchElementException:
        title = ''
    data = {
        'title': [title],
        'views': [to_number(get_str_from_xpath(xpath['views']))],
        'downloads': [to_number(get_str_from_xpath(xpath['downloads']))],
        'likes': [to_number(get_str_from_xpath(xpath['likes']))],
        'upload date': [get_date(get_str_from_xpath(xpath['upload date']))]
    }
    return pd.DataFrame(data, index=[content_url])


class ThreadedDrivers:
    def __init__(self, n_threads, main_logger):
        self.loggers = [setup_logger(str(i)) for i in range(n_threads)]
        self.drivers = [create_driver(self.loggers[i]) for i in range(n_threads)]
        self.locks = [t.Lock() for _ in range(n_threads)]
        self.main_logger = main_logger

    def acquire_lock(self):
        for i, lock in enumerate(self.locks):
            if lock.acquire(blocking=False):
                self.loggers[i].info(f'Lock {i} acquired')
                return i
        raise IndexError('No more drivers available')

    def func_wrapper(self, function, array):
        n_lock = self.acquire_lock()
        driver = self.drivers[n_lock]
        logger = self.loggers[n_lock]
        result = function(driver, logger, array)
        self.locks[n_lock].release()
        self.loggers[n_lock].info(f'Lock {n_lock} released')
        return result

    def map(self, function, array):
        f = partial(ThreadedDrivers.func_wrapper, self, function)
        max_chunksize = max(len(array)//(len(self.drivers)), 1)
        chunksize = min(max_chunksize, 2000)
        self.main_logger.info(f'Array length: {len(array)}')
        self.main_logger.info(f'Max chunksize: {max_chunksize}')
        self.main_logger.info(f'chunksize: {chunksize}')
        with ThreadPoolExecutor(max_workers=len(self.drivers)) as executor:
            return pd.concat(chain.from_iterable(executor.map(f, array, chunksize=chunksize)))


def main():
    main_logger = setup_logger('main')
    artists_urls_file = sys.argv[1] if len(
        sys.argv) > 1 else 'artists_urls.csv'
    data_filename = sys.argv[2] if len(sys.argv) > 2 else 'data.csv'
    data_path = Path('.') / data_filename
    artists_urls = np.loadtxt(artists_urls_file, dtype=str, ndmin=1)
    if data_path.exists():
        df = pd.read_csv(str(data_path))
        completed = df['artist url'].unique()
        artists_urls = artists_urls[~np.isin(artists_urls, completed)]

    n_threads = n_physical_cores * 3
    main_logger.info(f'Using {n_threads} threads')
    
    n_splits = math.ceil(len(artists_urls) / 5)
    artists_splits = np.array_split(artists_urls, n_splits)
    drivers = ThreadedDrivers(n_threads, main_logger)

    try:
        for artists_split in artists_splits:
            main_logger.info(f'Scraping collections of the following artists:\n{artists_split}')
            collections = drivers.map(get_collections_urls, artists_split)
            if len(collections) == 0:
                main_logger.info('No collections in this split')
                continue
            main_logger.info('Scraping content urls from collections')
            content = drivers.map(get_content_urls, collections['collection url'])
            main_logger.info('Scraping content statistics')
            stats = drivers.map(get_content_stats, content['content url'])
            main_logger.info('Joining data')
            joined_df = (
                collections
                .join(content, on='collection url', how='right')
                .join(stats, on='content url', how='left')
            )
            joined_df.index.name = 'artist url'
            header = False if data_path.exists() else True
            main_logger.info(f'Saving data to "{str(data_path)}"')
            joined_df.to_csv(data_path, header=header, mode='a')
    finally:
        main_logger.info('Closing web drivers')
        for driver in drivers.drivers:
            driver.quit()
        main_logger.info('All web drivers savely closed')


if __name__ == '__main__':
    main()
