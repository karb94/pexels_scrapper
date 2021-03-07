import pytest
import logging
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

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

@pytest.fixture
def bad_url():
    return 'https://www.pexels.com/video/cute-blonde-drinking-young-4720604/'

@pytest.fixture
def nan_stats_df():
    data = {
        'title': [np.nan],
        'views': [np.nan],
        'downloads': [np.nan],
        'likes': [np.nan],
        'upload date': [np.nan]
    }
    return pd.DataFrame(data)

def test_get_content_stats(bad_url, nan_stats_df, driver, logger):
    test_df = nan_stats_df.reindex([bad_url])
    print(test_df)
    pexels_scraper.logger = logger
    # with pytest.raises(NoSuchElementException):
    df = pexels_scraper.get_content_stats(driver, bad_url)
    assert_frame_equal(df, test_df)
