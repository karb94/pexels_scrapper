#! /bin/env python3

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException 
import pandas as pd
import numpy as np
import datetime
import multiprocessing as mp
import psutil
from operator import methodcaller
from functools import partial
from itertools import chain
import time
import re
import sys
from pathlib import Path
import logging

format='%(asctime)s    %(message)s'
datefmt='%Y/%m/%d %H:%M:%S'
logging.basicConfig(
    filename='pexels_scraper.log',
    format=format,
    datefmt=datefmt,
    level=logging.INFO
)

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
    url = artist_url + '/collections/'
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    artist_name = driver.find_element_by_tag_name('h1').text
    logging.info(f'COLLECTIONS from "{artist_name}":')
    matches = soup.find_all('a', {'class': 'discover__collections__collection'})
    collections_dirs = list(map(methodcaller('get', 'href'), matches))
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
    print('Collection:', collection_name, end='...')
    logging.info(f'CONTENT from "{collection_name}" collection')

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    photo_pattern = re.compile('\d+(?=\sphoto)')
    video_pattern = re.compile('\d+(?=\svideo)')
    photo_pattern = re.compile('\d+(?=\sphoto)')
    collection_info = soup.find_all('p', {'class': 'title-centered__more'})
    match = re.search(photo_pattern, collection_info[-1].get_text())
    n_photos = 0 if match is None else int(match.group(0))
    match = re.search(video_pattern, collection_info[-1].get_text())
    n_videos = 0 if match is None else int(match.group(0))

    video_class = 'js-photo-link js-photo-item__link photo-item__link'
    photo_class = 'js-photo-link photo-item__link'
    photos_count = 0
    videos_count = 0
    while  not (photos_count >= n_photos and videos_count >= n_videos):
        scroll_height = driver.execute_script("return document.body.scrollHeight;")  
        driver.execute_script("window.scrollTo(0, {scroll_distance});".format(scroll_distance=scroll_height))  
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        photos = soup.find_all('a', {'class': photo_class})
        videos = soup.find_all('a', {'class': video_class})
        photos_count = len(photos)
        videos_count = len(videos)

    videos_dirs = list(map(methodcaller('get', 'href'),  videos))
    content_dirs = list(map(methodcaller('get', 'href'), chain(photos, videos)))
    data = {
        'collection name': [collection_name] * len(content_dirs),
        'content url': content_dirs
    }
    index = [collection_url] * len(content_dirs)
    df = pd.DataFrame(data, index=index)
    df['content url'] = 'https://www.pexels.com' + df['content url']
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
    driver.get(content_url)
    xpath = {
        'button': '//*[@id="photo-page-body"]/div/div/section[1]/div[1]/button[2]',
        'title': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/h1/strong',
        'views': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[2]/div/div[1]/div/div/div[2]/div',
        'likes': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[2]/div',
        'downloads': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[1]/div[2]/div[3]/div/div[1]/div',
        'upload date': '//*[@id="photo-page-body"]/div/div/section[1]/div[2]/div/div[2]/div[1]/div[2]/div/small'
    }
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath['button']))).click()
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, xpath['views'])))
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
    return pd.DataFrame(data, index=[content_url])

def apply_to_split(function, split):
    driver = create_driver()
    f = partial(function, driver)
    # not_empty = lambda array: array.size!=0
    # result = pd.concat(filter(not_empty, map(f, split)))
    result = pd.concat(map(f, split))
    driver.quit()
    return result

def parallel_apply(function, array, pool):
    n_splits = n_logical_cores if len(array) > n_logical_cores else len(array)
    splits = np.array_split(array, n_splits)
    jobs = []
    for split in splits:
        jobs.append(pool.apply_async(apply_to_split, (function, split)))
    return pd.concat(map(methodcaller('get'), jobs))

def main():
    artists_urls_file = sys.argv[1] if len(sys.argv) > 1 else 'artists_urls.csv'
    data_file = sys.argv[2] if len(sys.argv) > 2 else 'data.csv'
    artists_urls_file = 'artists_urls.csv'
    data = {}
    artists_urls = np.loadtxt(artists_urls_file, dtype=str)

    logging.info(f'Using {n_logical_cores} CPU processors')
    pool = mp.Pool(processes=n_logical_cores)
    
    collections = parallel_apply(get_collections_urls, artists_urls, pool)
    logging.info('Finished fetching collections')
    content = parallel_apply(get_content_urls, collections['collection url'], pool)
    logging.info('Finished fetching content')
    stats = parallel_apply(get_content_stats, content['content url'], pool)
    logging.info('Finished gathering content stats')
    pool.close()
    pool.join()
    final_df = (
        collections
        .join(content, on='collection url', how='right')
        .join(stats, on='content url', how='left')
     )
    final_df.to_csv(data_file)

if __name__ == '__main__':
    main()

