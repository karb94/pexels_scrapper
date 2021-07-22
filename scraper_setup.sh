#! /bin/sh

COMMAND='
git clone https://github.com/karb94/pexels_scrapper.git &&
cd pexels_scrapper &&
python3 -m venv env &&
. env/bin/activate &&
pip install --upgrade pip &&
pip install -r requirements.txt &&
curl -sSO 'https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip' &&
# curl -sSO 'https://chromedriver.storage.googleapis.com/91.0.4472.19/chromedriver_linux64.zip' &&
unzip chromedriver_linux64.zip && rm chromedriver_linux64.zip &&
mv chromedriver env/bin/ &&
rm -f data.csv &&
setsid -f python3 pexels_scraper2.py >output 2>&1'

gcloud compute ssh "$1" --command="$COMMAND"
