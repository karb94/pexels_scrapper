import pytest
from selenium.common.exceptions import (NoSuchElementException,
    TimeoutException, ElementClickInterceptedException)
import logging

import pexels_scraper

# def pytest_configure():
#     logger = logging.getLogger()

@pytest.fixture
def driver():
    driver = pexels_scraper.create_driver()
    yield driver
    driver.quit()

@pytest.fixture
def logger():
    return logging.getLogger()

def test_get_content_stats(driver, logger):
    pexels_scraper.logger = logger
    url = 'https://www.pexels.com/video/cute-blonde-drinking-young-4720604/'
    with pytest.raises(NoSuchElementException):
        print('hefs')
        _ = pexels_scraper.get_content_stats(driver, url)
