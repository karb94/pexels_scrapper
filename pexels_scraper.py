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
import multiprocessing as mp
import psutil
from operator import methodcaller
from functools import partial
from itertools import chain
import math
import time
import re
import sys
from pathlib import Path
import logging

logs_dir = Path('./logs')
logs_dir.mkdir(exist_ok=True)

def setup_logger(name):
    log_file = logs_dir / (name + '.log')
    handler = logging.FileHandler(log_file)        
    logfmt='[%(asctime)s] %(levelname)s: %(message)s'
    datefmt='%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(logfmt, datefmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

n_logical_cores = psutil.cpu_count(logical=False)

def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = True
    chrome_options.javascriptEnabled = True
    chrome_options.browserTimeout = 0
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('window-size=1920,1080')
    chrome_options.add_argument('seleniumProtocol=WebDriver')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def get_collections_urls(driver, artist_url):
    collections_url = artist_url + '/collections/'
    driver.get(collections_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    artist_name = driver.find_element_by_tag_name('h1').text
    logger.info(f'COLLECTIONS from "{artist_name}":')
    matches = soup.find_all('a', {'class': 'discover__collections__collection'})
    not_likes = lambda collection: 'likes' not in collection
    collections_dirs = list(filter(not_likes, map(methodcaller('get', 'href'), matches)))
    data = {
        'artist name': [artist_name] * len(collections_dirs),
        'collection url': collections_dirs
    }
    index = [artist_url] * len(collections_dirs)
    df = pd.DataFrame(data, index=index)
    df['collection url'] = 'https://www.pexels.com' + df['collection url']
    return df

def get_content_urls(driver, collection_url):
    driver.get(collection_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    collection_name = soup.find('h1').get_text().strip('\n')
    artist_name = driver.find_element_by_tag_name('span').text
    logger.info(f'FETCHING CONTENT from "{artist_name}" in "{collection_name}" collection')

    old_scroll_height = 0
    new_scroll_height = driver.execute_script("return document.body.scrollHeight;")  
    while old_scroll_height < new_scroll_height:
        old_scroll_height = new_scroll_height
        driver.execute_script(f"window.scrollTo(0, {old_scroll_height});")  
        time.sleep(1)
        new_scroll_height = driver.execute_script("return document.body.scrollHeight;")  

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    video_class = 'js-photo-link js-photo-item__link photo-item__link'
    photo_class = 'js-photo-link photo-item__link'
    photos = soup.find_all('a', {'class': photo_class})
    videos = soup.find_all('a', {'class': video_class})
    videos_dirs = list(map(methodcaller('get', 'href'),  videos))
    content_dirs = list(map(methodcaller('get', 'href'), chain(photos, videos)))
    data = {
        'collection name': [collection_name] * len(content_dirs),
        'content url': content_dirs
    }
    index = [collection_url] * len(content_dirs)
    df = pd.DataFrame(data, index=index)
    df['content url'] = 'https://www.pexels.com' + df['content url']
    logger.info(f'GOT CONTENT from "{artist_name}" in "{collection_name}" collection')
    return df

def to_number(string):
    d = {
        'K': 1000,
        'M': 1000000,
        'B': 1000000000
    }
    if string[-1] in list(d.keys()):
        for key, val in d.items():
            if key in string:
                number = int(float(string.strip(key)) * val)
                break
    else:
        number = int(string)
    return number

def get_content_stats(driver, content_url):
    logger.info('FETCHING stats...')
    driver.get(content_url)
    xpath = {
        'button': '//*[@id="photo-page-body"]/div/div/section[1]/div[1]/button[2]',
        'title': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/h1/strong',
        'views': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[2]/div',
        'likes': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[2]/div',
        'downloads': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[1]/div',
        'upload date': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/small'
    }
    while True:
        try:
            (WebDriverWait(driver, 10)
             .until(EC.element_to_be_clickable((By.XPATH, xpath['button'])))
             .click())
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath['views'])))
            break
        except (ElementClickInterceptedException, TimeoutException) as e :
            logger.WARNING(f'Warning, exception: {e}')
            logger.exception(f'Exception: {e}')
            driver.get('about:blank')
            time.sleep(2)
            driver.get(content_url)
    get_str_from_xpath = lambda xpath: driver.find_element_by_xpath(xpath).text
    get_date = lambda string: datetime.datetime.strptime(string, "Uploaded at %B %d, %Y").strftime('%Y-%m-%d')
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
    logger.info('DONE')
    return pd.DataFrame(data, index=[content_url])

def apply_to_split(function, split):
    logger.info(f'SPLIT:\n{split}')
    while True:
        try:
            driver = create_driver()
            logger.info('WEB DRIVER initialised')
            break
        except:
            logger.info('TIMEOUT ERROR')
            logger.exception('')
    f = partial(function, driver)
    result = pd.concat(map(f, split))
    driver.quit()
    return result

def parallel_apply(function, array, pool, n_splits=None):
    if n_splits is None:
        n_splits = 4*n_logical_cores if len(array) > 4*n_logical_cores else len(array)
    splits = np.array_split(array, n_splits)
    jobs = []
    for split in splits:
        jobs.append(pool.apply_async(apply_to_split, (function, split)))
    return pd.concat(map(methodcaller('get'), jobs))

def setup_process_logger():
    log_name = mp.current_process().name
    global logger
    logger = setup_logger(log_name)
    logger.info(f'Processes {log_name} initialised')

def main():
    main_logger = setup_logger('main')
    artists_urls_file = sys.argv[1] if len(sys.argv) > 1 else 'artists_urls.csv'
    data_filename = sys.argv[2] if len(sys.argv) > 2 else 'data.csv'
    data_path = Path('.') / data_filename
    data = {}
    artists_urls = np.loadtxt(artists_urls_file, dtype=str)
    if data_path.exists():
        df = pd.read_csv(str(data_path))
        completed = df['artist url'].unique()
        artists_urls = artists_urls[~np.isin(artists_urls, completed)]

    main_logger.info(f'Using {n_logical_cores} CPU processors')
    pool = mp.Pool(processes=n_logical_cores, initializer=setup_process_logger)

    n_splits = math.ceil(len(artists_urls) / 5)
    artists_splits = np.array_split(artists_urls, n_splits)
    for artists_split in artists_splits:
        main_logger.info(f'SCRAPING the following artists:\n{artists_split}')
        collections = parallel_apply(get_collections_urls, artists_split, pool)
        if len(collections) == 0:
            main_logger.info('No collections in this split')
            continue
        main_logger.info('FINISHED scraping for collections urls')
        content = parallel_apply(get_content_urls,
                                 collections['collection url'], pool)
        main_logger.info('FINISHED scraping for content urls')
        stats = parallel_apply(get_content_stats, content['content url'], pool)
        main_logger.info('FINISHED scraping content stats')
        joined_df = (
            collections
            .join(content, on='collection url', how='right')
            .join(stats, on='content url', how='left')
         )
        joined_df.index.name = 'artist url'
        header = False if data_path.exists() else True
        main_logger.info(f'SAVING data to "{str(data_path)}"')
        joined_df.to_csv(data_path, header=header, mode='a')

    pool.close()
    pool.join()

if __name__ == '__main__':
    main()

