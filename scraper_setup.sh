#! /bin/sh

git clone https://github.com/karb94/pexels_scrapper.git
cd pexels_scrapper
curl -sSO https://chromedriver.storage.googleapis.com/91.0.4472.19/chromedriver_linux64.zip &&
    unzip chromedriver_linux64.zip && rm chromedriver_linux64.zip
python3 -m venv env
. env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
setsid -f python3 pexels_scraper2.py > output 2>&1 &
