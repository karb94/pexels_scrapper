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

def get_collections_urls(artist_url):
    url = artist_url + '/collections/'
    driver = drivers[0]
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    artist_name = driver.find_element_by_tag_name('h1').text
    logging.info(f'Collections from {artist_name}:')
    collections = soup.find_all('a', {'class': 'discover__collections__collection'})
    collections_dir = map(methodcaller('get', 'href'), collections)
    collection_title_class = 'discover__collections__collection__content__title'
    collection_titles_raw = soup.find_all('div', {'class': collection_title_class})
    collection_titles = map(methodcaller('get_text'), collection_titles_raw)

    collections_urls = pd.Series(collections_dir, name='collection_url')
    collections_urls =  'https://www.pexels.com' + collections_urls
    return artist_name, collections_urls

def get_content_urls(url):
    driver = drivers[0]
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    collection_name = soup.find('h1').get_text().strip('\n')
    logging.info(f'    {collection_name}')
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    photo_pattern = re.compile('\d+(?=\sphoto)')
    video_pattern = re.compile('\d+(?=\svideo)')
    collection_info = soup.find_all('p', {'class': 'title-centered__more'})
    match = re.search(photo_pattern, collection_info[-1].get_text())
    n_photos = 0 if match is None else int(match.group(0))
    match = re.search(video_pattern, collection_info[-1].get_text())
    n_videos = 0 if match is None else int(match.group(0))
    
    video_class = 'js-photo-link js-photo-item__link photo-item__link'
    photo_class = 'js-photo-link photo-item__link'
    photos_count = 0
    videos_count = 0
    while  photos_count != n_photos or videos_count != n_videos:
        scroll_height = driver.execute_script("return document.body.scrollHeight;")  
        driver.execute_script("window.scrollTo(0, {scroll_distance});".format(scroll_distance=scroll_height))  
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        photos = soup.find_all('a', {'class': photo_class})
        videos = soup.find_all('a', {'class': video_class})
        photos_count = len(photos)
        videos_count = len(videos)
        
    index = [url]*(n_photos+n_videos)
    content_urls =  pd.Series(map(methodcaller('get', 'href'), chain(photos, videos)), index=index, name='content url')
    content_urls = 'https://www.pexels.com' + content_urls
    return collection_name, content_urls

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

def get_stats(url, driver):
    driver.get(url)
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
        'title': title,
        'views': to_number(get_str_from_xpath(xpath['views'])),
        'downloads': to_number(get_str_from_xpath(xpath['downloads'])),
        'likes': to_number(get_str_from_xpath(xpath['likes'])),
        'upload date': get_date(get_str_from_xpath(xpath['upload date']))
    }
    
    return pd.Series(data)

def parallel_apply(n_driver, series):
    # series, driver = series_driver
    driver = drivers[n_driver]
    df = series.apply(get_stats, args=(driver,))
    return pd.concat([series, df], axis=1)
    
def parallel_get_stats(urls):
    processes = len(drivers) if len(urls) > len(drivers) else len(urls)
    urls_splits = np.array_split(urls, processes)
    pool = mp.Pool(processes=processes)
    jobs = []
    for n_driver, urls_split in enumerate(urls_splits):
        jobs.append(pool.apply_async(parallel_apply, (n_driver, urls_split)))
    results = map(methodcaller('get'), jobs)
    pool.close()
    pool.join()
    return pd.concat(results)

def get_collection_stats(collection_url):
    collection_name, content_urls = get_content_urls(collection_url)
    df = parallel_get_stats(content_urls)
    df.insert(loc=0, column='collection', value=collection_name)
    type_pattern = r'https://www.pexels.com/([^/]*)/'
    df.insert(loc=2, column='content type', value=df['content url'].str.extract(type_pattern))
    return df

def get_artist_stats(artist_url):
    artist_name, collections_urls = get_collections_urls(artist_url)
    df = pd.concat(map(get_collection_stats, collections_urls))
    df.insert(loc=1, column='collection url', value=df.index)
    df.reset_index(drop=True, inplace=True)
    df.insert(loc=0, column='artist name', value=artist_name)
    df.insert(loc=1, column='artist url', value=artist_url)
    return df

def create_drivers(hub_url='http://192.168.1.107:4444/wd/hub', n_drivers=None):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = False
    chrome_options.javascriptEnabled = True
    chrome_options.browserTimeout = 0
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('window-size=1920,1080')
    chrome_options.add_argument('seleniumProtocol=WebDriver')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")
    if n_drivers is None:
        n_drivers = psutil.cpu_count(logical=True)
    logging.info(f'Using {n_drivers} CPU processor')
    global drivers
    drivers = [webdriver.Chrome(options=chrome_options)
               for _ in range(n_drivers)]

def save_to_file(df, file_path):
    file = Path(file_path)
    header = not file.exists()
    df.to_csv(file, mode='a', header=header, index=False)

def main():
    artists_urls_file = sys.argv[1] if len(sys.argv) > 1 else 'artists_urls.csv'
    data_file = sys.argv[2] if len(sys.argv) > 2 else 'data.csv'

    artists_urls = pd.read_csv(artists_urls_file, header=None, squeeze=True)
    n_cpus = psutil.cpu_count(logical=True)
    n_cpus = 1
    create_drivers(n_drivers=n_cpus)

    logging.info('Starting Pexels web scraper')
    logging.info(f'Reading artists urls from "{artists_urls_file}"')
    logging.info(f'Data will be saved to "{data_file}"')

    for artist_url in artists_urls:
        df = get_artist_stats(artist_url) 
        save_to_file(df, data_file)

    for driver in drivers:
        driver.quit()


if __name__ == '__main__':
    main()

