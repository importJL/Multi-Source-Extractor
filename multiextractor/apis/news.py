from dotenv import load_dotenv
import urllib.request
import os
import json
import requests
from bs4 import BeautifulSoup
from multiextractor.transforms.soup_funcs import locate_elements

load_dotenv()

GNEWS_KEY = os.getenv('GNEWS_API')

def extract_news(**params) -> dict:
    '''
    Extract news articles with GNews API
    
    :params:
    **params: dict - custom parameters entered to build query string of URL for news extraction
    '''
    category = params.get('category', 'technology')
    language = params.get('language', 'en')
    country = params.get('country', 'us')
    max_articles = params.get('max_articles', 10)
    
    url = "https://gnews.io/api/v4/top-headlines?category={category}&lang={language}&country={country}&max={max_articles}&apikey={key}".format(
        category=category, language=language, country=country, max_articles=max_articles, key=GNEWS_KEY
    )
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode("utf-8"))
        articles = data["articles"]
    return data, articles
    
def extract_content(url: str, element_search: str, method: str, headers: dict[str, str] | None = None):
    '''
    Extract news articles using conventional requests & BeautifulSoup
    
    :params:
    url: str - webiste URL to scrape articles from
    element_search: str - search string to be used to identify specific elements for extraction as per method used
    method: str - method of searching.  Values given include `select` and `find`.
    headers: dict[str, str] -  optional, URL headers to be input into request API for additional protocol specifications on scraping
    '''
    r = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    return locate_elements(soup, element_search, method)